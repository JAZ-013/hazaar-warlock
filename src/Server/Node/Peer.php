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
            //'X-WARLOCK-ACCESS-KEY' => $this->access_key,
            //'X-WARLOCK-CLIENT-TYPE' => 'peer',
            //'X-WARLOCK-PEER-NAME' => Master::$instance->config->cluster['name']
        );

        $this->out = true;

        return $this->conn->connect(ake($this->options, 'host'), ake($this->options, 'port', 8000), $headers);

    }

    public function disconnect(){

        $this->active = false;

        if($this->out !== true)
            return parent::disconnect();

        $this->log->write(W_DEBUG, $this->type . "->DISCONNECT: HOST={$this->conn->address} PORT={$this->conn->port}", $this->name);

        return true;

    }

    public function __recv(&$buf){

        if($this->online !== true){

            $this->frameBuffer .= $buf;

            $this->active = false;

            if(!$this->initiateHandshake($this->frameBuffer))
                return false;

            $this->log->write(W_DEBUG, "WEBSOCKETS<-ACCEPT: HOST=$this->address PORT=$this->port PEER=$this->id", $this->name);

            $this->online = true;

            $buf = $this->frameBuffer;

            $this->frameBuffer = '';

        }

        return parent::recv($buf);

    }

    protected function processCommand($command, $payload = null){

        if (!$command)
            return false;

        if($this->active === false){

            if($command === 'OK'){

                $this->id = ake($payload, 'peer', 'UNKNOWN');

                $this->log->write(W_NOTICE, "Link to peer $this->id is now online at $this->address:$this->port", $this->name);

                return $this->active = true;

            }

            throw new \Exception('Command ' . $command . ' when peer is not online!');

        }

        switch($command){

            case 'NOOP':

                $this->log->write(W_INFO, 'NOOP: ' . print_r($payload, true), $this->name);

                return true;

            case 'OK':

                $this->log->write(W_NOTICE, 'OK', $this->name);

                return true;

            case 'ERROR':

                if($this->active !== true)
                    $this->log->write(W_ERR, 'Error initiating peer connection', $this->name);

                return true;

        }

        return Master::$cluster->processCommand($this, $command, $payload);

    }

}
