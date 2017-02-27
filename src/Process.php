<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

/*
 * Service Status Codes
 */
define('HAZAAR_SERVICE_ERROR', -1);

define('HAZAAR_SERVICE_INIT', 0);

define('HAZAAR_SERVICE_READY', 1);

define('HAZAAR_SERVICE_RUNNING', 2);

define('HAZAAR_SERVICE_SLEEP', 3);

define('HAZAAR_SERVICE_STOPPING', 4);

define('HAZAAR_SERVICE_STOPPED', 5);

define('HAZAAR_SCHEDULE_DELAY', 0);

define('HAZAAR_SCHEDULE_INTERVAL', 1);

define('HAZAAR_SCHEDULE_NORM', 2);

define('HAZAAR_SCHEDULE_CRON', 3);

abstract class Process extends WebSockets {

    protected $id;

    protected $key;

    protected $socket;

    protected $application;

    protected $protocol;

    protected $subscriptions = array();

    //WebSocket Buffers
    protected $frameBuffer;

    protected $payloadBuffer;

    protected $closing            = false;

    function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol) {

        parent::__construct(array(
            'warlock'
        ));

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = guid();

        $this->key = uniqid();

    }

    final function __destruct(){

        $this->disconnect(true);

    }

    public function connect($application_name, $port, $job_id = null, $access_key = null){

        if(!($this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP)))
            throw new \Exception('Unable to create TCP socket!');

        $host = '127.0.0.1';

        if(!socket_connect($this->socket, $host, $port))
            throw new \Exception('Unable to connect to localhost:' . $port);

        /**
         * Initiate a WebSockets connection
         */
        $handshake = $this->createHandshake('/' . $application_name .'/warlock?CID=' . $this->id, $host, null, $this->key);

        socket_write($this->socket, $handshake, strlen($handshake));

        /**
         * Wait for the response header
         */
        $read = array($this->socket);

        $write = $except = null;

        $sockets = socket_select($read, $write, $except, 3000);

        if($sockets == 0) return FALSE;

        socket_recv($this->socket, $buf, 65536, 0);

        $response = $this->parseHeaders($buf);

        if($response['code'] != 101)
            throw new \Exception('Walock server returned status: ' . $response['code'] . ' ' . $response['status']);

        if(! $this->acceptHandshake($response, $responseHeaders, $this->key))
            throw new \Exception('Warlock server denied our connection attempt!');

        return true;

    }

    protected function disconnect() {

        if($this->socket) {

            if($this->closing === false) {

                $this->closing = true;

                $frame = $this->frame('', 'close');

                socket_write($this->socket, $frame, strlen($frame));

                $this->recv($payload);

            }

            socket_close($this->socket);

            $this->socket = null;

            return TRUE;

        }

        return FALSE;

    }

    public function connected() {

        return is_resource($this->socket);

    }

    protected function setErrorHandler($methodName) {

        if(! method_exists($this, $methodName))
            throw new \Exception('Unable to set error handler.  Method does not exist!', E_ALL);

        return set_error_handler(array($this, $methodName));

    }

    protected function setExceptionHandler($methodName) {

        if(! method_exists($this, $methodName))
            throw new \Exception('Unable to set exception handler.  Method does not exist!');

        return set_exception_handler(array($this, $methodName));

    }

    protected function processFrame(&$frameBuffer) {

        if ($this->frameBuffer) {

            $frameBuffer = $this->frameBuffer . $frameBuffer;

            $this->frameBuffer = null;

            return $this->processFrame($frameBuffer);

        }

        if (!$frameBuffer)
            return FALSE;

        $opcode = $this->getFrame($frameBuffer, $payload);

        /**
         * If we get an opcode that equals FALSE then we got a bad frame.
         *
         * If we get a opcode of -1 there are more frames to come for this payload. So, we return FALSE if there are no
         * more frames to process, or TRUE if there are already more frames in the buffer to process.
         */
        if ($opcode === FALSE) {

            $this->disconnect(true);

            return FALSE;

        } elseif ($opcode === -1) {

            $this->payloadBuffer .= $payload;

            return (strlen($frameBuffer) > 0);

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

                $frame = $this->frame('', 'pong', FALSE);

                socket_write($this->socket, $frame, strlen($frame));

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

    protected function send($command, $payload = null) {

        if(! $this->socket)
            return FALSE;

        $packet = $this->protocol->encode($command, $payload);

        $frame = $this->frame($packet, 'text');

        $len = strlen($frame);

        $bytes_sent = socket_write($this->socket, $frame, $len);

        if($bytes_sent == -1) {

            throw new \Exception('An error occured while sending to the socket');

        } elseif($bytes_sent != $len) {

            throw new \Exception($bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected');

        }

        return TRUE;

    }

    protected function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0) {

        if(! $this->socket)
            return FALSE;

        $read = array(
            $this->socket
        );

        $write = $except = null;

        if(socket_select($read, $write, $except, $tv_sec, $tv_usec) > 0) {

            // will block to wait server response
            $bytes_received = socket_recv($this->socket, $buffer, 65536, 0);

            if($bytes_received == -1) {

                throw new \Exception('An error occured while receiving from the socket');

            } elseif($bytes_received == 0) {

                throw new \Exception('Received response of zero bytes.');

            }

            if($frame = $this->processFrame($buffer))
                return $this->protocol->decode($frame, $payload);

        }

        return null;

    }

    protected function processCommand($command, $payload = null) {

        switch($command) {

            case 'EVENT':

                if(! (array_key_exists('id', $payload) && array_key_exists($payload['id'], $this->subscriptions)))
                    return FALSE;

                try {

                    $func = $this->subscriptions[$payload['id']];

                    if(is_string($func))
                        $func = array($this, $func);

                    if(is_callable($func))
                        call_user_func_array($func, array(ake($payload, 'data'), $payload));

                }
                catch(\Exception $e) {

                    error_log('ERROR: ' . $e->getMessage());

                }

                break;

            case 'STATUS':

                $this->sendHeartbeat();

                break;

            case 'PONG':

                $trip_ms = (microtime(true) - $payload) * 1000;

                $this->send('DEBUG', 'PONG received in ' . $trip_ms . 'ms');

                break;

            default:

                $this->send('DEBUG', 'Unhandled command: ' . $command);

                break;

        }

        return true;

    }

    protected function ping(){

        return $this->send('ping', microtime(true));

    }

    protected function subscribe($event, $callback, $filter = null) {

        if(! method_exists($this, $callback))
            return FALSE;

        $this->subscriptions[$event] = $callback;

        return $this->send('subscribe', array('id' => $event, 'filter' => $filter));

    }

    protected function unsubscribe($event) {

        if(! array_key_exists($event, $this->subscriptions))
            return FALSE;

        unset($this->subscriptions[$event]);

        return $this->send('unsubscribe', array('id' => $event));

    }

    protected function trigger($event, $payload) {

        return $this->send('trigger', array('id' => $event, 'data' => $payload));

    }

}
