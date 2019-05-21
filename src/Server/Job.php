<?php

namespace Hazaar\Warlock\Server;

/**
 * STATUS CONSTANTS
 */
define('STATUS_INIT', 0);

define('STATUS_QUEUED', 1);

define('STATUS_QUEUED_RETRY', 2);

define('STATUS_STARTING', 3);

define('STATUS_RUNNING', 4);

define('STATUS_COMPLETE', 5);

define('STATUS_CANCELLED', 6);

define('STATUS_ERROR', 7);

abstract class Job extends \Hazaar\Model\Strict {

    private $protocol;

    protected $log;

    static private $job_ids = array();

    private $__status;

    private $__subscriptions = array();

    /*
     * This method simple increments the jids integer but makes sure it is unique before returning it.
     */
    public function getJobId() {

        $count = 0;

        $jid = NULL;

        while(in_array($jid = uniqid(), Job::$job_ids)) {

            if ($count >= 10)
                throw new \Exception("Unable to generate job ID after $count attempts . Giving up . This is bad! ");

        }

        Job::$job_ids[] = $jid;

        return $jid;

    }

    public function init(){

        return array(
            'id' => array(
                'type' => 'string',
                'default' => $this->getJobID()
            ),
            'type' => 'string',
            'status' => array(
                'type' => 'int',
                'default' => STATUS_INIT,
                'update' => array(
                    'post' => function(){
                        $this->log->write(W_DEBUG, 'STATUS: ' . strtoupper($this->status()), $this->id);
                    }
                )
            ),
            'tag' => 'string',
            'info' => 'string',
            'access_key' => array(
                'type' => 'string',
                'value' => uniqid()
            ),
            'start' => array(
                'type' => 'int',
                'default' => time()
            ),
            'application' => array(
                'type' => 'model',
                'items' => array(
                    'path' => array(
                        'type' => 'string',
                        'default' => APPLICATION_PATH
                    ),
                    'env' => array(
                        'type' => 'string',
                        'default' => APPLICATION_ENV
                    )
                )
            ),
            'retries' => array(
                'type' => 'int',
                'default' => 0
            ),
            'expire' => array(
                'type' => 'int',
                'default' => 0
            ),
            'respawn' => array(
                'type' => 'boolean',
                'default' => false
            ),
            'respawn_delay' => array(
                'type' => 'int',
                'default' => 0
            ),
            'restarts' => array(
                'type' => 'int',
                'default' => 0
            ),
            'params' => array(
                'type' => 'array',
                'default' => array()
            ),
            'process' => array(
                'type' => 'Hazaar\Warlock\Server\Process'
            ),
            'last_heartbeat' => array(
                'type' => 'int'
            ),
            'heartbeats' => array(
                'type' => 'int',
                'default' => 0
            ),
            'loglevel' => array()
        );

    }

    final public function construct(){

        $this->log = new Logger();

    }

    final public function destruct(){

        if(($index = array_search($this->id, Job::$job_ids)) !== false)
            unset(Job::$job_ids[$index]);

    }

    public function status() {

        switch ($this->status) {

            case STATUS_QUEUED :
                $ret = 'queued';
                break;

            case STATUS_QUEUED_RETRY :
                $ret = 'queued (restart)';
                break;

            case STATUS_STARTING :
                $ret = 'starting';
                break;

            case STATUS_RUNNING :
                $ret = 'running';
                break;

            case STATUS_COMPLETE :
                $ret = 'complete';
                break;

            case STATUS_CANCELLED :
                $ret = 'cancelled';
                break;

            case STATUS_ERROR :
                $ret = 'error';
                break;

            default :
                $ret = 'invalid';
                break;
        }

        return $ret;

    }

    public function ready(){

        return (($this->status === STATUS_QUEUED || $this->status === STATUS_QUEUED_RETRY) && time() >= $this->start);

    }

    /**
     * Returns boolean indicating if a job has expired
     *
     * Expired jobs are completed jobs that have an expire param > 0 or are in an error state, and the expire time has passed.
     * @return boolean
     */
    public function expired(){

        return ($this->status === STATUS_ERROR
            || ($this->status === STATUS_COMPLETE && $this->expire > 0 && time() >= $this->expire));

    }

    public function cancel($expire = 30){

        $this->status = STATUS_CANCELLED;

        $this->expire = time() + $expire;

        $this->send('cancel');

    }

    public function sendEvent($event_id, $trigger_id, $data) {

        if (!in_array($event_id, $this->__subscriptions)) {

            $this->log->write(W_WARN, "Client $this->id is not subscribe to event $event_id", $this->name);

            return false;

        }

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(true),
            'data' => $data
        );

        return $this->send('EVENT', $packet);

    }

    public function recv(){

        while($packet = $this->process->recv()){

            $this->log->write(W_DECODE, "JOB<-PACKET: " . trim($packet, "\n"), $this->name);

            $payload = null;

            $time = null;

            if($type = Master::$protocol->decode($packet, $payload, $time)){

                if (!$this->processCommand($type, $payload, $time))
                    throw new \Exception('Negative response returned while processing command: ' . $type);

            }

        }

    }

    public function send($command, $payload = NULL) {

        if (!is_string($command))
            return false;

        $packet = Master::$protocol->encode($command, $payload); //Override the timestamp.

        $this->log->write(W_DECODE, "JOB->PACKET: $packet", $this->name);

        return $this->process->write($packet);

    }

    private function processCommand($command, $payload = null){

        if (!$command)
            return false;

        $this->log->write(W_DEBUG, "JOB<-COMMAND: $command ID=$this->id", $this->name);

        switch($command){

            case 'NOOP':

                $this->log->write(W_INFO, 'NOOP: ' . print_r($payload, true), $this->name);

                return true;

            case 'OK':

                if($payload)
                    $this->log->write(W_INFO, $payload, $this->name);

                return true;

            case 'ERROR':

                $this->log->write(W_ERR, $payload, $this->name);

                return true;

            case 'SUBSCRIBE' :

                $filter = (property_exists($payload, 'filter') ? $payload->filter : NULL);

                return $this->commandSubscribe($payload->id, $filter);

            case 'UNSUBSCRIBE' :

                return $this->commandUnsubscribe($payload->id);

            case 'TRIGGER' :

                return $this->commandTrigger($payload->id, ake($payload, 'data'), ake($payload, 'echo', false));

            case 'LOG':

                return $this->commandLog($payload);

            case 'DEBUG':

                $this->log->write(W_DEBUG, ake($payload, 'data'), $this->name);

                return true;

            case 'STATUS' :

                if($payload)
                    return $this->commandStatus($payload);

            default:

                return Master::$instance->processCommand($this, $command, $payload);

        }

        return false;

    }

    private function commandSubscribe($event_id, $filter = NULL) {

        $this->log->write(W_NOTICE, "JOB->SUBSCRIBE: EVENT=$event_id ID=$this->id", $this->name);

        $this->__subscriptions[] = $event_id;

        return Master::$instance->subscribe($this, $event_id, $filter);

    }

    public function commandUnsubscribe($event_id) {

        $this->log->write(W_DEBUG, "JOB->UNSUBSCRIBE: EVENT=$event_id ID=$this->id", $this->name);

        if(($index = array_search($event_id, $this->subscriptions)) !== false)
            unset($this->__subscriptions[$index]);

        return Master::$instance->unsubscribe($this, $event_id);

    }

    public function commandTrigger($event_id, $data, $echo_client = true) {

        $this->log->write(W_NOTICE, "JOB->TRIGGER: NAME=$event_id ID=$this->id ECHO=" . strbool($echo_client), $this->name);

        return Master::$instance->trigger($event_id, $data, ($echo_client === false ? $this->id : null));

    }

    private function commandLog(\stdClass $payload){

        if(!property_exists($payload, 'msg'))
            throw new \Exception('Unable to write to log without a log message!');

        $level = ake($payload, 'level', W_INFO);

        $name = ake($payload, 'name', $this->name);

        if(is_array($payload->msg)){

            foreach($payload->msg as $msg)
                $this->commandLog((object)array('level' => $level, 'msg' => $msg, 'name' => $name));

        }else{

            $this->log->write($level, ake($payload, 'msg', '--'), $name);

        }

        return true;

    }

    private function commandStatus(\stdClass $payload = null) {

        $this->__status = $payload;

        return true;

    }

}
