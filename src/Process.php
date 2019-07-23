<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

abstract class Process {

    /**
     * The connection object
     * @var \Hazaar\Warlock\Connection\_Interface
     */
    protected $conn;

    protected $id;

    protected $application;

    protected $protocol;

    function __construct(\Hazaar\Application $application, Protocol $protocol, $guid = null) {

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = ($guid === null ? guid() : $guid);

        if(!($this->conn = $this->connect($protocol, $guid)) instanceof Connection\_Interface)
            throw new \Exception('Process initialisation failed!', 1);

    }

    function __destruct(){

        if($this->conn instanceof Connection\_Interface)
            $this->conn->disconnect();

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

    protected function connect(\Hazaar\Warlock\Protocol $protocol, $guid = null){

        return null;

    }

    protected function connected(){

        if(!$this->conn)
            return false;

        return $this->conn->connected();

    }

    public function send($command, $payload = null) {

        if(!$this->conn)
            return false;

        return $this->conn->send($command, $payload);

    }

    public function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0) {

        if(!$this->conn)
            return false;

        return $this->conn->recv($payload, $tv_sec, $tv_usec);

    }

    protected function __processCommand($command, $payload = null) {

        switch($command) {

            case 'EVENT':

                if(!($payload && property_exists($payload, 'id') && array_key_exists($payload->id, $this->subscriptions)))
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

        if(($ret = $this->recv($payload)) !== $command){

            $msg = "KVSTORE: Invalid response to command $command from server: " . var_export($ret, true);

            if(is_object($payload) && property_exists($payload, 'reason'))
                $msg .= "\nError: $payload->reason";

            throw new \Exception($msg);

        }

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

    /**
     * @brief Execute code from standard input in the application context
     *
     * @detail This method is will accept Hazaar Protocol commands from STDIN and execute them.
     *
     * Exit codes:
     *
     * * 1 - Bad Payload - The execution payload could not be decoded.
     * * 2 - Unknown Payload Type - The payload execution type is unknown.
     * * 3 - Service Class Not Found - The service could not start because the service class could not be found.
     * * 4 - Unable to open control channel - The application was unable to open a control channel back to the execution server.
     *
     * @since 1.0.0
     */
    static public function runner(\Hazaar\Application $application, $service_name = null) {

        if(!class_exists('\Hazaar\Warlock\Config'))
            throw new \Exception('Could not find default warlock config.  How is this even working!!?');

        $defaults = \Hazaar\Warlock\Config::$default_config;

        $defaults['sys']['id'] = crc32(APPLICATION_PATH);

        $warlock = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, $defaults);

        define('RESPONSE_ENCODED', $warlock->server->encoded);

        $protocol = new Protocol($warlock->sys->id, $warlock->server->encoded);

        if(is_string($service_name)){

            $service = self::getServiceClass($service_name, $application, $protocol);

            if(!$service instanceof \Hazaar\Warlock\Service)
                die("Could not find service named '$service_name'.\n");

            $code = call_user_func(array($service, 'main'));

        }else{

            //Execution should wait here until we get a command
            $line = fgets(STDIN);

            $code = 1;

            $payload = null;

            if($type = $protocol->decode($line, $payload)){

                if(!$payload instanceof \stdClass)
                    throw new \Exception('Got Hazaar protocol packet without payload!');

                //Synchronise the timezone with the server
                if($tz = ake($payload, 'timezone'))
                    date_default_timezone_set($tz);

                switch ($type) {

                    case 'EXEC' :

                        $container = new \Hazaar\Warlock\Container($application, $protocol);

                        $code = $container->exec($payload->exec, ake($payload, 'params'));

                        break;

                    case 'SERVICE' :

                        if(!property_exists($payload, 'name')) {

                            $code = 3;

                            break;

                        }

                        if($config = ake($payload, 'config'))
                            $application->config->extend($config);

                        $service = self::getServiceClass($payload->name, $application, $protocol);

                        if($service instanceof \Hazaar\Warlock\Service){

                            $code = call_user_func(array($service, 'main'), ake($payload, 'params'), ake($payload, 'dynamic', false));

                        } else {

                            $code = 3;

                        }

                        break;

                    default:

                        $code = 2;

                        break;

                }

            }

        }

        return $code;

    }

    static public function getServiceClass($service_name, \Hazaar\Application $application, \Hazaar\Warlock\Protocol $protocol){

        $class_search = array(
            'Application\\Service\\' . ucfirst($service_name),
            ucfirst($service_name) . 'Service'
        );

        $service = null;

        foreach($class_search as $service_class){

            if(!class_exists($service_class))
                continue;

            $service = new $service_class($application, $protocol, true);

        }

        return $service;

    }

}
