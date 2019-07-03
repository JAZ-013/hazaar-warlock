<?php

namespace Hazaar\Warlock\Server;

use \Hazaar\Warlock\Server\Connection;

abstract class Node {

    /**
     * @var Connection
     */
    public $conn;

    /**
     * @var boolean
     */
    public $closing = false;

    /**
     * Buffer for fragmented frames
     * @var string
     */
    public $frameBuffer = NULL;

    /**
     * Buffer for payloads split over multiple frames
     * @var string
     */
    public $payloadBuffer = NULL;

    /*
     * Warlock specific stuff
     */
    private $cluster;

    public $id;

    public $log;

    public $type = 'CLIENT';  //Possible types are 'CLIENT', 'SERVICE' or 'ADMIN'.

    public $name;

    public $since;

    public $status;

    /**
     * Any detected time offset. This doesn't need to be exact so we don't bother worrying about latency.
     * @var int
     */
    public $offset = 0;

    function __construct(Connection $conn, $type, $options = array()) {

        $this->id = guid();

        $this->conn = $conn;

        $this->log = Master::$instance->log;

        $this->type = strtoupper($type);

        $this->since = time();

    }

    function __destruct(){

        $this->log->write(W_DEBUG, $this->type . "->DESTROY: ID=$this->id", $this->name);

    }

    public function disconnect(){

        $this->log->write(W_DEBUG, $this->type . "->DISCONNECT: ID=$this->id", $this->name);

        $this->conn->disconnect();

        $this->conn = null;

        Master::$cluster->removeNode($this);

        return true;

    }

    public function init($headers){

        $this->log->write(W_DEBUG, $this->type . "->INIT: ID=$this->id", $this->name);

        $this->lastContact = time();

        return true;

    }

    public function processCommand($command, $payload = null){

        return false;

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

    public function send($command, $payload = null, $frame_id = null) {

        if(!($packet = Master::$protocol->encode($command, $payload, $frame_id))) //Override the timestamp.
            return false;

        $this->log->write(W_DECODE, $this->type . "->PACKET: $packet", $this->name);

        return $this->conn->send($packet);

    }

}
