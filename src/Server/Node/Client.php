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

        if(array_key_exists('x-warlock-access-key', $headers)){

            if(!\Hazaar\Warlock\Server\Master::$cluster->authorise($this, (object)array('access_key' => base64_decode($headers['x-warlock-access-key']))))
                return false;

        }

        if($username = ake($headers,'x-warlock-user')){

            if(($this->username = base64_decode($username)) === NULL)
                return false;

            $this->log->write(W_NOTICE, "USER: $this->username");

        }

        if(ake($headers, 'x-warlock-php') === 'true')
            return true;

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

    public function processCommand($command, $payload = null){

        if (!$command)
            return false;

        switch($command){

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

        }

        return false;

    }

}
