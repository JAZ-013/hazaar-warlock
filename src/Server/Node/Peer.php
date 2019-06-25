<?php

namespace Hazaar\Warlock\Server\Node;

use \Hazaar\Warlock\Server\Master;

define('WARLOCK_PEER_OFFLINE', 0);

define('WARLOCK_PEER_CONNECT', 1);

define('WARLOCK_PEER_INIT', 2);

define('WARLOCK_PEER_HANDSHAKE', 3);

define('WARLOCK_PEER_ONLINE', 4);

/**
 * Cluster short summary.
 *
 * Cluster description.
 *
 * @version 1.0
 * @author JamieCarl
 */
class Peer extends \Hazaar\Warlock\Server\Node {

    /**
     * Warlock protocol access key
     * @var string
     */
    private $access_key;

    /**
     * WebSocket handshake key
     *
     * @var string
     */
    private $key;

    private $out = false;

    private $options;

    private $status;

    private $status_time;

    private $status_names = array(
        0 => 'WARLOCK_PEER_OFFLINE',
        1 => 'WARLOCK_PEER_CONNECT',
        2 => 'WARLOCK_PEER_INIT',
        3 => 'WARLOCK_PEER_HANDSHAKE',
        4 => 'WARLOCK_PEER_ONLINE'
    );

    public function __construct($conn = null, $options = array()){

        if($conn === null)
            $conn = new \Hazaar\Warlock\Server\Connection($this);

        parent::__construct($conn, 'PEER', $options);

        $this->options = $options;

        $this->status = WARLOCK_PEER_OFFLINE;

    }

    public function connected(){

        return $this->conn->connected();

    }

    public function disconnect(){

        $this->status = WARLOCK_PEER_OFFLINE;

        if($this->out !== true)
            return parent::disconnect();

        $this->log->write(W_DEBUG, $this->type . "->DISCONNECT: HOST={$this->conn->address} PORT={$this->conn->port}", $this->name);

        return true;

    }

    public function auth($name, $access_key){

        if(ake($this->options, 'access_key') !== base64_decode($access_key))
            return false;

        $this->id = $name;

        return true;

    }

    public function init($params){

        if($name = ake($params, 'x-warlock-peer-name'))
            $this->id = $name;

        $this->setStatus(WARLOCK_PEER_ONLINE);

        $this->lastContact = time();

        $this->log->write(W_INFO, "Peer '$this->id' is now online!");

        return true;

    }

    public function online(){

        return ($this->conn->connected() && $this->status === WARLOCK_PEER_ONLINE);

    }

    private function setStatus($status){

        $this->log->write(W_DEBUG, "PEER->STATUS: " . $this->status_names[$status], $this->name);

        $this->status = $status;

        $this->status_when = time();

    }

    public function ping(){

        if($this->status === WARLOCK_PEER_ONLINE){

            if($this->conn->connected())
                return true;

            $this->setStatus(WARLOCK_PEER_OFFLINE);

        }

        if($this->status === WARLOCK_PEER_OFFLINE){

            if($this->conn->connected() === true){

                $this->setStatus(WARLOCK_PEER_CONNECT);

            }else{

                $this->out = true;

                $this->conn->setNode($this);

                if($this->conn->connect(ake($this->options, 'host'), ake($this->options, 'port', 8000)))
                    $this->setStatus(WARLOCK_PEER_CONNECT);

            }

        }

        if($this->status === WARLOCK_PEER_CONNECT){

            if($this->conn->connected() !== true){ //Waiting for connection

                if(time() >= ($this->status_when + $this->options['timeout'])){

                    $this->log->write(W_NOTICE, 'Connection timed out after ' . $this->options['timeout'] . ' seconds.', $this->name);

                    $this->conn->disconnect();

                    $this->setStatus(WARLOCK_PEER_OFFLINE);

                    return false;

                }

                return true;

            }

            $this->log->write(W_DEBUG, "PEER->CONNECT: HOST={$this->conn->address} PORT={$this->conn->port}", $this->name);

            Master::$instance->addConnection($this->conn);

            $this->setStatus(WARLOCK_PEER_INIT);

        }

        if($this->status === WARLOCK_PEER_INIT){

            if($this->conn->connected() !== true){

                $this->setStatus(WARLOCK_PEER_OFFLINE);

                return false;

            }

            $headers = array(
                'X-WARLOCK-PHP' => 'true',
                'X-WARLOCK-ACCESS-KEY' => base64_encode(ake($this->options, 'access_key')),
                'X-WARLOCK-CLIENT-TYPE' => 'peer',
                'X-WARLOCK-PEER-NAME' => $this->name
            );

            $this->conn->initHandshake($headers);

            $this->setStatus(WARLOCK_PEER_HANDSHAKE);

        }

        if($this->status === WARLOCK_PEER_HANDSHAKE){

            //TODO: Check a handshake timeout

        }

        return true;

    }

}
