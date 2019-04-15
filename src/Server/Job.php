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

    private $client;

    protected $log;

    static private $job_ids = array();

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

    public function registerClient(Client $client){

        $this->log->write(W_NOTICE, 'Client ' . $client->id . ' registered as control channel.', $this->id);

        $client->type = $this->type;

        $client->jobs[$this->id] = $this;

        $client->log->setLevel($this->loglevel);

        $this->client = $client;

        return true;

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

        if($this->client)
            $this->client->send('cancel');

    }

    public function sendEvent($event_id, $trigger_id, $data = null){

        return $this->client->sendEvent($event_id, $trigger_id, $data);

    }

}