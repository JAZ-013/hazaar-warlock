<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

require_once('Constants.php');

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

    public    $bytes_received = 0;

    function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol) {

        parent::__construct(array('warlock'));

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = guid();

        $this->key = uniqid();

    }

    final function __destruct(){

        $this->disconnect(true);

    }

    public function connect($application_name, $host, $port, $job_id = null, $access_key = null){

        if(!($this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP)))
            throw new \Exception('Unable to create TCP socket!');

        if(@!socket_connect($this->socket, $host, $port)){

            socket_close($this->socket);

            $error = socket_last_error($this->socket);

            $this->socket = null;

            throw new \Exception(socket_strerror($error));

        }

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

        //If we have a job_id and access_key we can register as a control channel
        if($job_id && $access_key){

            $this->send('SYNC', array(
                'client_id' => $this->id,
                'user' => base64_encode(get_current_user()),
                'job_id' => $job_id,
                'access_key' => $access_key
            ));

            if($this->recv() != 'OK'){

                $this->send('ERROR', 'Service was unable to register control channel');

                return false;

            }

        }

        return true;

    }

    public function disconnect() {

        $this->frameBuffer = '';

        if($this->socket) {

            if($this->closing === false) {

                $this->closing = true;

                $frame = $this->frame('', 'close');

                socket_write($this->socket, $frame, strlen($frame));

                $this->recv($payload);

            }

            if($this->socket)
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

    protected function processFrame(&$frameBuffer = null) {

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

        if(!($packet = $this->protocol->encode($command, $payload)))
            return false;

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

        //Process any frames sitting in the local frame buffer first.
        while($frame = $this->processFrame()){

            if($frame === true)
                break;

            return $this->protocol->decode($frame, $payload);

        }

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

                return $this->disconnect();

            }

            //if(time() > ($start + 3))
            if(($start++) > 5)
                return false;

        }

        return null;

    }

    protected function __processCommand($command, $payload = null) {

        switch($command) {

            case 'EVENT':

                if(! (array_key_exists('id', $payload) && array_key_exists($payload['id'], $this->subscriptions)))
                    return FALSE;

                $func = $this->subscriptions[$payload['id']];

                if(is_string($func))
                    $func = array($this, $func);

                if(is_callable($func)){

                    $process = true;

                    if(is_object(($obj = ake($func, 0)))
                        && method_exists($obj, 'beforeEvent'))
                        $process = $this->beforeEvent($payload);

                    if($process !== false){

                        $result = call_user_func_array($func, array(ake($payload, 'data'), $payload));

                        if($result !== false
                            && is_object(($obj = ake($func, 0)))
                            && method_exists($obj, 'afterEvent'))
                            $this->afterEvent($payload);

                    }

                }

                break;

            case 'PONG':

                $trip_ms = (microtime(true) - $payload) * 1000;

                $this->send('DEBUG', 'PONG received in ' . $trip_ms . 'ms');

                break;

            case 'OK':

                break;

            default:

                $this->send('DEBUG', 'Unhandled command: ' . $command);

                break;

        }

        return true;

    }

    public function ping($wait_pong = false){

        $ret = $this->send('PING', microtime(true));

        if(!$wait_pong)
            return $ret;

        return $this->recv();

    }

    public function subscribe($event, $callback, $filter = null) {

        if(! method_exists($this, $callback))
            return FALSE;

        $this->subscriptions[$event] = $callback;

        return $this->send('SUBSCRIBE', array('id' => $event, 'filter' => $filter));

    }

    public function unsubscribe($event) {

        if(! array_key_exists($event, $this->subscriptions))
            return FALSE;

        unset($this->subscriptions[$event]);

        return $this->send('UNSUBSCRIBE', array('id' => $event));

    }

    public function trigger($event, $data = NULL, $echo_self = false) {

        $packet = array(
            'id' => $event,
            'echo' => $echo_self
        );

        if($data)
            $packet['data'] = $data;

        return $this->send('TRIGGER', $packet);

    }

    public function log($level, $message){

        if(!is_int($level))
            return false;

        return $this->send('LOG', array('level' => $level, 'msg' => $message));

    }

}
