<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock\Connection;

final class Socket extends \Hazaar\Warlock\Protocol\WebSockets implements _Interface {

    protected $id;

    protected $key;

    protected $socket;

    protected $connected = false;

    protected $protocol;

    //WebSocket Buffers
    protected $frameBuffer;

    protected $payloadBuffer;

    protected $closing            = false;

    public    $bytes_received = 0;

    public    $socket_last_error = null;

    function __construct(\Hazaar\Warlock\Protocol $protocol, $guid = null) {

        if(! extension_loaded('sockets'))
            throw new \Exception('The sockets extension is not loaded.');

        parent::__construct(array('warlock'));

        $this->start = time();

        $this->protocol = $protocol;

        $this->id = ($guid === null ? guid() : $guid);

        $this->key = uniqid();

    }

    final function __destruct(){

        $this->disconnect(true);

    }

    public function connect($application_name, $host, $port, $extra_headers = null){

        $this->socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if(!is_resource($this->socket))
            throw new \Exception('Unable to create TCP socket!');

        if(!($this->connected = @socket_connect($this->socket, $host, $port))){

            $this->socket_last_error = socket_last_error($this->socket);

            error_clear_last();
            
            socket_close($this->socket);

            $this->socket = null;

            return false;

        }

        $headers = array(
            'X-WARLOCK-PHP' => 'true',
            'X-WARLOCK-USER' => base64_encode(get_current_user())
        );

        if(is_array($extra_headers))
            $headers = array_merge($headers, $extra_headers);

        /**
         * Initiate a WebSockets connection
         */
        $handshake = $this->createHandshake('/' . $application_name .'/warlock?CID=' . $this->id, $host, null, $this->key, $headers);

        @socket_write($this->socket, $handshake, strlen($handshake));

        /**
         * Wait for the response header
         */
        $read = array($this->socket);

        $write = $except = null;

        $sockets = socket_select($read, $write, $except, 3000);

        if($sockets == 0) return false;

        socket_recv($this->socket, $buf, 65536, 0);

        $response = $this->parseHeaders($buf);

        if($response['code'] != 101)
            throw new \Exception('Walock server returned status: ' . $response['code'] . ' ' . $response['status']);

        if(!$this->acceptHandshake($response, $responseHeaders, $this->key))
            throw new \Exception('Warlock server denied our connection attempt!');

        return true;

    }

    public function getLastSocketError($as_string = false){

        return ($as_string ? socket_strerror($this->socket_last_error) : $this->socket_last_error);

    }

    public function disconnect() {

        $this->frameBuffer = '';

        if($this->socket) {

            if($this->closing === false) {

                $this->closing = true;

                $frame = $this->frame('', 'close');

                @socket_write($this->socket, $frame, strlen($frame));

                $this->recv($payload);

            }

            if($this->socket)
                socket_close($this->socket);

            $this->socket = null;

            return true;

        }

        return false;

    }

    public function connected() {

        return is_resource($this->socket);

    }

    protected function processFrame(&$frameBuffer = null) {

        if ($this->frameBuffer) {

            $frameBuffer = $this->frameBuffer . $frameBuffer;

            $this->frameBuffer = null;

            return $this->processFrame($frameBuffer);

        }

        if (!$frameBuffer)
            return false;

        $opcode = $this->getFrame($frameBuffer, $payload);

        /**
         * If we get an opcode that equals false then we got a bad frame.
         *
         * If we get a opcode of -1 there are more frames to come for this payload. So, we return false if there are no
         * more frames to process, or true if there are already more frames in the buffer to process.
         */
        if ($opcode === false) {

            $this->disconnect(true);

            return false;

        } elseif ($opcode === -1) {

            $this->frameBuffer .= $frameBuffer;

            return (strlen($this->frameBuffer) > 0);

        }

        switch ($opcode) {

            case 0 :
            case 1 :
            case 2 :

                break;

            case 8 :

                if($this->closing === false)
                    $this->disconnect();

                return false;

            case 9 :

                $frame = $this->frame('', 'pong', false);

                @socket_write($this->socket, $frame, strlen($frame));

                return false;

            case 10 :

                return false;

            default :

                $this->disconnect();

                return false;

        }

        if (strlen($frameBuffer) > 0) {

            $this->frameBuffer = $frameBuffer;

            $frameBuffer = '';

        }

        if ($this->payloadBuffer) {

            $payload = $this->payloadBuffer . $payload;

            $this->payloadBuffer = '';

        }

        return $payload;

    }

    public function send($command, $payload = null) {

        if(! $this->socket)
            return false;

        if(!($packet = $this->protocol->encode($command, $payload)))
            return false;

        $frame = $this->frame($packet, 'text');

        $len = strlen($frame);

        $attempts = 0;

        $total_sent = 0;

        while($frame){

            $attempts++;

            $bytes_sent = @socket_write($this->socket, $frame, $len);

            if($bytes_sent === -1 || $bytes_sent === false)
                throw new \Exception('An error occured while sending to the socket');

            $total_sent += $bytes_sent;

            if($total_sent === $len) //If all the bytes sent then don't waste time processing the leftover frame
                break;

            if($attempts >= 100)
                throw new \Exception('Unable to write to socket.  Socket appears to be stuck.');

            $frame = substr($frame, $bytes_sent);

        }

        return true;

    }

    public function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0) {

        //Process any frames sitting in the local frame buffer first.
        while($frame = $this->processFrame()){

            if($frame === true)
                break;

            return $this->protocol->decode($frame, $payload);

        }

        if(!$this->socket)
            exit(4);

        if(socket_get_option($this->socket, SOL_SOCKET, SO_ERROR) > 0)
            exit(4);

        $read = array(
            $this->socket
        );

        $write = $except = null;

        $start = 0;//time();

        while(socket_select($read, $write, $except, $tv_sec, $tv_usec) > 0) {

            // will block to wait server response
            $this->bytes_received += $bytes_received = socket_recv($this->socket, $buffer, 65536, 0);

            if($bytes_received > 0) {

                if(($frame = $this->processFrame($buffer)) === true)
                    continue;

                if($frame === false)
                    break;

                return $this->protocol->decode($frame, $payload);

            }elseif($bytes_received == -1) {

                throw new \Exception('An error occured while receiving from the socket');

            } elseif($bytes_received == 0) {

                $this->disconnect();

                return false;

            }

            if(($start++) > 5)
                return false;

        }

        return null;

    }

}
