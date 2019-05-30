<?php

namespace Hazaar\Warlock\Server;

/**
 * Cluster short summary.
 *
 * Cluster description.
 *
 * @version 1.0
 * @author JamieCarl
 */
class Peer extends Client implements CommInterface {

    private $access_key;

    private $key;

    private $connected = false;

    private $online = false;

    private $buffer = '';

    public function __construct($options){

        parent::__construct();

        $this->log = Master::$instance->log;

        $this->id = guid();

        $this->key = uniqid();

        $this->address = ake($options, 'host');

        $this->port = ake($options, 'port', 8000);

        $this->access_key = base64_encode(ake($options, 'access_key'));

        $this->type = 'PEER';

    }

    function __destruct(){

        $this->log->write(W_DEBUG, "PEER->DESTROY: HOST=$this->address PORT=$this->port", $this->name);

    }

    public function connect(){

        if($this->online === true)
            throw new \Exception('Already connected and online with peer!');

        $this->log->write(W_INFO, 'Connecting to peer at ' . $this->address . ':' . $this->port);

        $socket = stream_socket_client('tcp://' . $this->address . ':' . $this->port, $errno, $errstr, 3, STREAM_CLIENT_CONNECT);

        $this->name = 'SOCKET#' . intval($socket);

        if(!$socket){

            $this->log->write(W_ERR, 'Error #' . $errno . ': ' . $errstr);

            return false;

        }

        $headers = array(
           'X-WARLOCK-PHP' => 'true',
           'X-WARLOCK-ACCESS-KEY' => $this->access_key,
           'X-WARLOCK-CLIENT-TYPE' => 'peer',
           'X-WARLOCK-PEER-NAME' => Master::$instance->config->cluster['name']
       );

        $handshake = $this->createHandshake('/' . APPLICATION_NAME . '/warlock?CID=' . $this->id, $this->address, null, $this->key, $headers);

        $this->log->write(W_DEBUG, "WEBSOCKETS->HANDSHAKE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        fwrite($socket, $handshake);

        return $this->socket = $socket;

    }

    public function connected(){

        return is_resource($this->socket);

    }

    public function disconnect(){

        if(!is_resource($this->socket))
            return false;

        $this->log->write(W_NOTICE, 'Peer ' . $this->address . ':'. $this->port . ' going offline');

        stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);

        $this->log->write(W_DEBUG, "CLIENT->CLOSE: HOST=$this->address PORT=$this->port", $this->name);

        fclose($this->socket);

        $this->connected = $this->online = false;

        $this->socket = null;

        return true;

    }

    public function recv(&$buf){

        if($this->connected !== true){

            $this->frameBuffer .= $buf;

            $this->online = false;

            if(!$this->initiateHandshake($this->frameBuffer))
                return false;

            $this->log->write(W_DEBUG, "WEBSOCKETS<-ACCEPT: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

            $this->connected = true;

            $buf = $this->frameBuffer;

            $this->frameBuffer = '';

        }

        return parent::recv($buf);

    }

    private function initiateHandshake(&$buf){

        if(($pos = strpos($buf, "\r\n\r\n")) === false)
            return null;

        if(!($response = $this->parseHeaders(substr($buf, 0, $pos))))
            return false;

        $buf = substr($buf, $pos + 4);

        if($response['code'] !== 101)
            throw new \Exception('Walock server returned status: ' . $response['code'] . ' ' . $response['status']);

        if(!$this->acceptHandshake($response, $responseHeaders, $this->key))
            throw new \Exception('Warlock server denied our connection attempt!');

        return true;

    }

    protected function processCommand($command, $payload = null){

        if (!$command)
            return false;

        $this->log->write(W_DEBUG, $this->type . "<-COMMAND: $command HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        if($this->online === false){

            if($command === 'OK'){

                $this->name = ake($payload, 'peer', 'UNKNOWN');

                $this->log->write(W_NOTICE, "Peer $this->name is now online at $this->address:$this->port", $this->name);

                return $this->online = true;

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

                if($this->online !== true)
                    $this->log->write(W_ERR, 'Error initiating peer connection', $this->name);

                return true;

        }

        return Master::$instance->processCommand($this, $command, $payload);

    }

}
