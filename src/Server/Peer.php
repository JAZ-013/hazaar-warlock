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

        parent::__construct(array('warlock'));

        $this->log = new \Hazaar\Warlock\Server\Logger();

        $this->id = guid();

        $this->key = uniqid();

        $this->host = ake($options, 'host');

        $this->port = ake($options, 'port', 8000);

        $this->access_key = base64_encode(ake($options, 'access_key'));

        $this->type = 'PEER';

    }

    function __destruct(){

        $this->log->write(W_DEBUG, "PEER->DESTROY: HOST=$this->host PORT=$this->port", $this->name);

    }

    public function connect(){

        if($this->online === true)
            throw new \Exception('Already connected and online with peer!');

        $this->log->write(W_INFO, 'Connecting to peer at ' . $this->host . ':' . $this->port);

        $socket = stream_socket_client('tcp://' . $this->host . ':' . $this->port, $errno, $errstr, 30, STREAM_CLIENT_CONNECT);

        if(!$socket){

            $this->log->write(W_ERR, 'Error #' . $errno . ': ' . $errstr);

            return false;

        }

        $headers = array(
            'X-WARLOCK-PHP' => 'true',
            'X-WARLOCK-ACCESS-KEY' => $this->access_key,
            'X-WARLOCK-CLIENT-TYPE' => 'peer'
        );

        $handshake = $this->createHandshake('/' . APPLICATION_NAME . '/warlock?CID=' . $this->id, $this->host, null, $this->key, $headers);

        $this->log->write(W_DEBUG, "WEBSOCKETS->HANDSHAKE: HOST=$this->host PORT=$this->port CLIENT=$this->id", $this->name);

        fwrite($socket, $handshake);

        return $this->socket = $socket;

    }

    public function connected(){

        return is_resource($this->socket);

    }

    public function disconnect(){

        if(!is_resource($this->socket))
            return false;

        $this->log->write(W_NOTICE, 'Peer ' . $this->host . ':'. $this->port . ' going offline');

        stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);

        $this->log->write(W_DEBUG, "CLIENT->CLOSE: HOST=$this->host PORT=$this->port", $this->name);

        fclose($this->socket);

        $this->connected = $this->online = false;

        $this->socket = null;

        return true;

    }

    public function send($command, $payload = null){

    }

    public function recv(&$buf){

        if($this->connected !== true){

            $this->frameBuffer .= $buf;

            $this->online = false;

            if(!$this->initiateHandshake($this->frameBuffer))
                return false;

            $this->log->write(W_DEBUG, "WEBSOCKETS<-ACCEPT: HOST=$this->host PORT=$this->port CLIENT=$this->id", $this->name);

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

        switch($command){

            case 'NOOP':

                $this->log->write(W_INFO, 'NOOP: ' . print_r($payload, true), $this->name);

                return true;

            case 'OK':

                if($this->online !== true){

                    $this->log->write(W_NOTICE, 'Peer ' . $this->host . ':'. $this->port . ' is now online');

                    $this->online = true;

                }

                return true;

            case 'ERROR':

                if($this->online !== true)
                    $this->log->write(W_ERR, 'Error initiating peer connection', $this->name);

                return true;

            default:

                $this->log->write(W_WARN, 'Received: ' . $command);

                break;

        }

    }

}
