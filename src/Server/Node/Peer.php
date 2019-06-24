<?php

namespace Hazaar\Warlock\Server\Node;

use \Hazaar\Warlock\Server\Master;

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

    /**
     * The peer is online and the cluster level protocol has been negotiated successfully
     * @var mixed
     */
    public $active = false;

    private $out = false;

    private $options;

    public function __construct($conn = null, $options = array()){

        if($conn === null)
            $conn = new \Hazaar\Warlock\Server\Connection($this);

        parent::__construct($conn, 'PEER', $options);

        if($this->out = (ake($options, 'host', false) !== false))
            $this->access_key = base64_encode(ake($options, 'access_key'));

        $this->options = $options;

    }

    public function connect(){

        if($this->out !== true)
            return false;

        $headers = array(
            'X-WARLOCK-PHP' => 'true',
            'X-WARLOCK-ACCESS-KEY' => $this->access_key,
            'X-WARLOCK-CLIENT-TYPE' => 'peer',
            'X-WARLOCK-PEER-NAME' => $this->name
        );

        $this->out = true;

        return $this->conn->connect(ake($this->options, 'host'), ake($this->options, 'port', 8000), $headers);

    }

    public function connected(){

        return $this->conn->connected();

    }

    public function disconnect(){

        $this->active = false;

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

        $this->log->write(W_INFO, "Peer '$this->id' is now online!");

        $this->lastContact = time();

        $this->active = true;

        return true;

    }

}
