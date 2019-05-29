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
class Peer extends \Hazaar\Warlock\Protocol\WebSockets implements CommInterface {

    public $name;

    private $id;

    private $host;

    private $port;

    private $access_key;

    private $protocol;

    private $log;

    /**
     * @var resource
     */
    private $socket;

    private $key;

    private $connected = false;

    private $online = false;

    private $buffer = '';

    public function __construct($config, \Hazaar\Warlock\Protocol $protocol){

        $this->log = new \Hazaar\Warlock\Server\Logger();

        $this->protocol = $protocol;

        $this->id = guid();

        $this->key = uniqid();

        $this->host = ake($config, 'host');

        $this->port = ake($config, 'port', 8000);

        $this->access_key = base64_encode(ake($config, 'access_key'));

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
            'X-WARLOCK-CLIENT-TYPE' => 'service'
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

        $this->buffer .= $buf;

        if($this->connected !== true){

            $this->online = false;

            if(!$this->initiateHandshake($this->buffer))
                return false;

            $this->log->write(W_DEBUG, "WEBSOCKETS<-ACCEPT: HOST=$this->host PORT=$this->port CLIENT=$this->id", $this->name);

            $this->connected = true;

        }

        if(strlen($this->buffer) > 0){

            $opcode = $this->getFrame($this->buffer, $packet);

            

            $this->log->write(W_DECODE, 'PEER<-PACKET: ' . $packet);

            $command = $this->protocol->decode($packet, $payload);

            $this->processCommand($command, $payload);

        }

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

    private function processCommand($command, $payload = null){

        switch($command){

            case 'OK':

                if($this->online !== true){

                    $this->log->write(W_NOTICE, 'Peer ' . $this->host . ':'. $this->port . ' is now online');

                    $this->online = true;

                }

                break;

            default:

                $this->log->write(W_WARN, 'Received: ' . $command);

                break;

        }

    }

}
