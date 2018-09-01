<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

require_once('Constants.php');

abstract class Process extends Protocol\WebSockets {

    protected $id;

    protected $job_id;

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

    public    $socket_last_error = null;

    function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol, $guid = null) {

        parent::__construct(array('warlock'));

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = ($guid === null ? guid() : $guid);

        $this->key = uniqid();

    }

    final function __destruct(){

        $this->disconnect(true);

    }

    public function connect($application_name, $host, $port, $extra_headers = null){

        if(array_key_exists('X-WARLOCK-JOB-ID', $extra_headers))
            $this->job_id = $extra_headers['X-WARLOCK-JOB-ID'];

        if (!extension_loaded('sockets'))
            throw new \Exception('The sockets extension is not loaded.');

        $this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if(!is_resource($this->socket))
            throw new \Exception('Unable to create TCP socket!');

        if(!@socket_connect($this->socket, $host, $port)){

            $this->socket_last_error = socket_last_error($this->socket);

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

        if(! $this->acceptHandshake($response, $responseHeaders, $this->key))
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

    protected function send($command, $payload = null) {

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

            @$bytes_sent = socket_write($this->socket, $frame, $len);

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

    protected function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0) {

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

                return $this->disconnect();

            }

            if(($start++) > 5)
                return false;

        }

        return null;

    }

    protected function __processCommand($command, $payload = null) {

        switch($command) {

            case 'EVENT':

                if(! (property_exists($payload, 'id') && array_key_exists($payload->id, $this->subscriptions)))
                    return false;

                $func = $this->subscriptions[$payload->id];

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

                if(is_int($payload)){

                    $trip_ms = (microtime(true) - $payload) * 1000;

                    $this->send('DEBUG', 'PONG received in ' . $trip_ms . 'ms');

                }else{

                    $this->send('ERROR', 'PONG received with invalid payload!');

                }

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
            return false;

        $this->subscriptions[$event] = $callback;

        return $this->send('SUBSCRIBE', array('id' => $event, 'filter' => $filter));

    }

    public function unsubscribe($event) {

        if(! array_key_exists($event, $this->subscriptions))
            return false;

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

    public function log($level, $message, $name = null){

        if($name === null)
            $name = $this->job_id;

        if(!(is_int($level) && is_string($message)))
            return false;

        return $this->send('LOG', array('level' => $level, 'msg' => $message, 'name' => $name));

    }

    public function debug($data, $name = null){

        return $this->send('DEBUG', array('data' => $data, 'name' => $name));

    }

    public function spawn($service, $params = array()){

        return $this->send('SPAWN', array('name' => $service, 'detach' => true, 'params' => $params));

    }

    public function kill($service){

        return $this->send('KILL', array('name' => $service));

    }

    private function __kv_send_recv($command, $data){

        if(!$this->send($command, $data))
            return false;

        $payload = null;

        if(($ret = $this->recv($payload)) !== $command)
            throw new \Exception('Invalid response from server: ' . $ret . (property_exists($payload, 'reason') ? ' (' . $payload->reason . ')' : null));

        return $payload;

    }

    public function get($key, $namespace = null) {

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVGET', $data);

    }

    public function set($key, $value, $timeout = NULL, $namespace = null) {

        $data = array('k' => $key, 'v' => $value);

        if($namespace)
            $data['n'] = $namespace;

        if($timeout !== null)
            $data['t'] = $timeout;

        return $this->__kv_send_recv('KVSET', $data);

    }

    public function has($key, $namespace = null) {

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVHAS', $data);

    }

    public function del($key, $namespace = null) {

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVDEL', $data);

    }

    public function clear($namespace = null) {

        $data = ($namespace ? array('n' => $namespace) : null);

        return $this->__kv_send_recv('KVCLEAR', $data);

    }

    public function list($namespace = null){

        $data = ($namespace ? array('n' => $namespace) : null);

        return $this->__kv_send_recv('KVLIST', $data);

    }

    public function pull($key, $namespace = null){

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVPULL', $data);

    }

    public function push($key, $value, $namespace = null){

        $data = array('k' => $key, 'v' => $value);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVPUSH', $data);

    }

    public function pop($key, $namespace = null){

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVPOP', $data);

    }

    public function shift($key, $namespace = null){

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVSHIFT', $data);

    }

    public function unshift($key, $value, $namespace = null){

        $data = array('k' => $key, 'v' => $value);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVUNSHIFT', $data);

    }

    public function incr($key, $step = null, $namespace = null){

        $data = array('k' => $key);

        if($step > 0)
            $data['s'] = $step;

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVINCR', $data);

    }

    public function decr($key, $step = null, $namespace = null){

        $data = array('k' => $key);

        if($step > 0)
            $data['s'] = $step;

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVDECR', $data);

    }

    public function keys($namespace = null){

        $data = array();

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVKEYS', $data);

    }

    public function vals($namespace = null){

        $data = array();

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVVALS', $data);

    }

    public function count($key, $namespace = null){

        $data = array('k' => $key);

        if($namespace)
            $data['n'] = $namespace;

        return $this->__kv_send_recv('KVCOUNT', $data);

    }

}
