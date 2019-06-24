<?php

namespace Hazaar\Warlock\Server\Node;

use \Hazaar\Warlock\Server\Connection;

class Client extends \Hazaar\Warlock\Server\Node {

    /**
     * This is an array of event_id and socket pairs
     * @var array
     */
    public $subscriptions = array();

    /**
     * If the client has any child jobs
     * @var mixed
     */
    public $jobs = array();

    private $username;

    function __construct(Connection $conn, $options = array()) {

        parent::__construct($conn, 'CLIENT', guid(), $options);

    }

    public function init($headers){

        if(!parent::init($headers))
            return false;

        if(array_key_exists('UID', $headers['url'])){

            if(($this->username = base64_decode($headers['url']['UID'])) === NULL)
                return false;

            $this->log->write(W_NOTICE, "USER: $this->username");

        }

        $init_packet = json_encode(\Hazaar\Warlock\Protocol::$typeCodes);

        if(\Hazaar\Warlock\Server\Master::$protocol->encoded())
            $init_packet = base64_encode($init_packet);

        return $this->conn->send($init_packet);

    }

    public function disconnect(){

        if(($count = count($this->jobs)) > 0){

            $this->log->write(W_NOTICE, 'Disconnected WebSocket client has ' . $count . ' running/pending child jobs', $this->name);

            foreach($this->jobs as $job){

                if($job->detach !== true)
                    $job->status = STATUS_CANCELLED;

            }

        }

        return parent::disconnect();

    }

    public function sendEvent($event_id, $trigger_id, $data) {

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(true),
            'data' => $data
        );

        return $this->send('EVENT', $packet);

    }

    public function processCommand($command, $payload = null){

        if (!$command)
            return false;

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

            case 'SYNC':

                return $this->commandSync($payload);

            case 'PING' :

                return $this->send('pong', $payload);

            case 'PONG':

                if(is_int($payload)){

                    $trip_ms = (microtime(true) - $payload) * 1000;

                    $this->log->write(W_INFO, 'PONG received in ' . $trip_ms . 'ms', $this->name);

                }else{

                    $this->log->write(W_WARN, 'PONG received with invalid payload!', $this->name);

                }

                break;

            case 'LOG':

                return $this->commandLog($payload);

            case 'DEBUG':

                $this->log->write(W_DEBUG, ake($payload, 'data'), $this->name);

                return true;

            case 'STATUS' :

                if($payload)
                    return $this->commandStatus($payload);

        }

        return false;

    }

    private function commandSync(\stdClass $payload){

        $this->log->write(W_DEBUG, $this->type . "<-SYNC: OFFSET=$this->offset HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        $response = null;

        if(!Master::$instance->authorise($this, $payload, $response)) {

            $this->log->write(W_WARN, 'Warlock control rejected to client ' . $this->id, $this->name);

            $this->send('ERROR');

            return false;

        }

        $type = strtoupper($payload->type);

        if($this->type !== $type){

            $this->log->write(W_NOTICE, "Client type changed from $this->type to $type.", $this->name);

            $this->type = $type;

        }

        $this->send('OK', $response);

        return true;

    }

    private function commandStatus(\stdClass $payload = null) {

        if($this->type !== 'SERVICE'){

            $this->log->write(W_WARN, 'Client sent status but client is not a service!', $this->address . ':' . $this->port);

            throw new \Exception('Status only allowed for services!');

        }

        $this->status = $payload;

        return true;

    }

    private function commandSubscribe($event_id, $filter = NULL) {

        $this->log->write(W_DEBUG, $this->type . "<-SUBSCRIBE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        $this->subscriptions[] = $event_id;

        return Master::$instance->subscribe($this, $event_id, $filter);

    }

    public function commandUnsubscribe($event_id) {

        $this->log->write(W_DEBUG, $this->type . "<-UNSUBSCRIBE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        if(($index = array_search($event_id, $this->subscriptions)) !== false)
            unset($this->subscriptions[$index]);

        return Master::$instance->unsubscribe($this, $event_id);

    }

    public function commandTrigger($event_id, $data, $echo_client = true) {

        $this->log->write(W_DEBUG, $this->type . "<-TRIGGER: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

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

}
