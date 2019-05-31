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
     * The socket is connected.  This must be maintained because we use ASYNC stream socket connections
     * @var boolean
     */
    private $connected = false;

    /**
     * The connection is online and WebSocket protocol has been negotiated successfully
     * @var boolean
     */
    private $online = false;

    /**
     * The peer is online and the cluster level protocol has been negotiated successfully
     * @var mixed
     */
    private $active = false;

    private $timeout;

    public function __construct($options){

        parent::__construct();

        $this->log = Master::$instance->log;

        $this->id = guid();

        $this->key = uniqid();

        $this->address = ake($options, 'host');

        $this->port = ake($options, 'port', 8000);

        $this->timeout = ake($options, 'timeout', 3);

        $this->access_key = base64_encode(ake($options, 'access_key'));

        $this->type = 'PEER';

    }

    function __destruct(){

        $this->log->write(W_DEBUG, "PEER->DESTROY: HOST=$this->address PORT=$this->port", $this->name);

    }

    public function connect(){

        if($this->active === true)
            throw new \Exception('Already connected and online with peer!');

        if($this->socket && stream_socket_get_name($this->socket, true) === false){

            stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);

            fclose($this->socket);

            $this->socket = null;

        }

        if(!$this->socket){

            $this->log->write(W_INFO, 'Connecting to peer at ' . $this->address . ':' . $this->port);

            $socket = stream_socket_client('tcp://' . $this->address . ':' . $this->port, $errno, $errstr, $this->timeout, STREAM_CLIENT_ASYNC_CONNECT);

            $this->name = 'SOCKET#' . intval($socket);

            if(!$socket){

                $this->log->write(W_ERR, 'Error #' . $errno . ': ' . $errstr);

                return false;

            }

            $this->socket = $socket;

            if(stream_socket_get_name($this->socket, true) === false)
                return false;

        }

        $this->connected = true;

        $headers = array(
           'X-WARLOCK-PHP' => 'true',
           'X-WARLOCK-ACCESS-KEY' => $this->access_key,
           'X-WARLOCK-CLIENT-TYPE' => 'peer',
           'X-WARLOCK-PEER-NAME' => Master::$instance->config->cluster['name']
       );

        $handshake = $this->createHandshake('/' . APPLICATION_NAME . '/warlock?CID=' . $this->id, $this->address, null, $this->key, $headers);

        $this->log->write(W_DEBUG, "WEBSOCKETS->HANDSHAKE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        fwrite($this->socket, $handshake);

        return $this->socket;

    }

    public function connected(){

        return $this->connected;

    }

    public function disconnect(){

        if(!is_resource($this->socket))
            return false;

        $this->log->write(W_NOTICE, "Link to peer $this->name going offline", $this->name);

        stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);

        $this->log->write(W_DEBUG, "CLIENT->CLOSE: HOST=$this->address PORT=$this->port", $this->name);

        fclose($this->socket);

        $this->connected = $this->online = $this->active = false;

        $this->socket = null;

        return true;

    }

    public function recv(&$buf){

        if($this->online !== true){

            $this->frameBuffer .= $buf;

            $this->active = false;

            if(!$this->initiateHandshake($this->frameBuffer))
                return false;

            $this->log->write(W_DEBUG, "WEBSOCKETS<-ACCEPT: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

            $this->online = true;

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

        return Master::$instance->processCommand($this, $command, $payload);

    }

}
