<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

require_once('Constants.php');

abstract class Process {

    protected $id;

    protected $job_id;

    protected $key;

    protected $application;

    protected $protocol;

    protected $subscriptions = array();

    protected $buffer;

    public    $bytes_received = 0;

    private $closing = false;

    function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol, $guid = null) {

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = ($guid === null ? guid() : $guid);

        $this->key = uniqid();

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

    public function send($command, $payload = null) {

        if(!($packet = $this->protocol->encode($command, $payload)))
            return false;

        $len = strlen($packet .= "\n");

        $attempts = 0;

        $total_sent = 0;

        while($packet){

            $attempts++;

            $bytes_sent = @fwrite(STDOUT, $packet, $len);

            if($bytes_sent === -1 || $bytes_sent === false)
                throw new \Exception('An error occured while sending to the socket');

            $total_sent += $bytes_sent;

            if($total_sent === $len) //If all the bytes sent then don't waste time processing the leftover frame
                break;

            if($attempts >= 100)
                throw new \Exception('Unable to write to socket.  Socket appears to be stuck.');

            $packet = substr($packet, $bytes_sent);

        }

        return true;

    }

    private function processPacket(&$buffer = null){

        echo $buffer;

        exit;

        if ($this->buffer) {

            $buffer = $this->buffer . $buffer;

            $this->buffer = null;

            return $this->processPacket($buffer);

        }

        if (!$buffer)
            return false;

        if(($pos = strpos($this->buffer, "\n")) === false)
            return true;

        $packet = substr($buffer, 0, $pos);

        echo $packet;

        exit;

        if (strlen($buffer) > $pos) {

            $this->buffer = substr($buffer, $pos);

            $buffer = '';

        }

        return $packet;

    }

    protected function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0) {

        while($packet = $this->processPacket()){

            if($packet === true)
                break;

            return $this->protocol->decode($packet, $payload);

        }

        $read = array(STDIN);

        $write = $except = null;

        while(stream_select($read, $write, $except, $tv_sec, $tv_usec) > 0) {

            // will block to wait server response
            $buffer = fread(STDIN, 65536);

            $this->bytes_received += ($bytes_received = strlen($buffer));

            if($bytes_received > 0) {

                if(($packet = $this->processPacket($buffer)) === true)
                    continue;

                if($packet === false)
                    break;

                return $this->protocol->decode($packet, $payload);

            }elseif($bytes_received === -1) {

                throw new \Exception('An error occured while receiving from the stream');

            }

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

        if(!is_int($level))
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
            throw new \Exception('Invalid response from server: ' . $ret . (is_object($payload) && property_exists($payload, 'reason') ? ' (' . $payload->reason . ')' : null));

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
