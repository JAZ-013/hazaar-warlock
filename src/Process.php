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

    static private $options = [
        'service_name' => [ 'n', 'name', 'service_name', "\tStart a service directly from the command line." ],
        'daemon' => ['d', 'daemon', null, "\t\t\t\tStart in daemon mode and wait for a startup packet." ],
        'help' => [ null, 'help', null, "\t\t\t\tPrint this message and exit." ]
    ];

    static private $opt;

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

        if(!is_string($name)) $name = null;

        return $this->send('LOG', array('level' => $level, 'msg' => $message, 'name' => $name));

    }

    public function debug($data, $name = null){

        if(!is_string($name)) $name = null;
        
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

    static private function getopt(){

        if(!self::$opt){

            self::$opt = [0 => '', 1 => []];

            foreach(self::$options as $name => $o){

                if($o[0]) self::$opt[0] .= $o[0] . (is_string($o[2]) ? ':' : '');

                if($o[1]) self::$opt[1][] = $o[1] . (is_string($o[2]) ? ':' : '');

            }

        }

        $ops = getopt(self::$opt[0], self::$opt[1]);

        $options = [];

        foreach(self::$options as $name => $o){

            $s = $l = false;

            $sk = $lk = null;

            if(($o[0] && ($s = array_key_exists($sk = rtrim($o[0], ':'), $ops))) || ($o[1] && ($l = array_key_exists($lk = rtrim($o[1], ':'), $ops))))
                $options[$name] = is_string($o[2]) ? ($s ? $ops[$sk] : $ops[$lk]) : true;

        }

        if(ake($options, 'help') === true)
            return self::showHelp();

        return $options;

    }

    static private function showHelp(){

        $script = basename($_SERVER['SCRIPT_FILENAME']);

        $msg = "Syntax: $script [options]\nOptions:\n";
        
        foreach(self::$options as $o){

            $avail = [];

            if($o[0]) $avail[] = '-' . $o[0] . (is_string($o[2]) ? ' ' . $o[2] : '');

            if($o[1]) $avail[] = '--' . $o[1] . (is_string($o[2]) ? '=' . $o[2] : '');

            $msg .= '  ' . implode(', ', $avail) . $o[3] . "\n";

        }

        echo $msg;

        return 0;

    }

    /**
     * @brief Execute code from standard input in the application context
     *
     * @detail This method will accept Hazaar Protocol commands from STDIN and execute them.
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
    static public function runner(\Hazaar\Application $application, $argv = null) {

        $options = self::getopt();

        $service_name = ake($options, 'service_name', null);

        if(!class_exists('\Hazaar\Warlock\Config'))
            throw new \Exception('Could not find default warlock config.  How is this even working!!?');

        $exitcode = 1;

        $_SERVER['WARLOCK_EXEC'] = 1;

        $defaults = \Hazaar\Warlock\Config::$default_config;

        $defaults['sys']['id'] = crc32(APPLICATION_PATH);

        $warlock = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, $defaults);

        define('RESPONSE_ENCODED', $warlock->server->encoded);

        $protocol = new Protocol($warlock->sys->id, $warlock->server->encoded);

        if(ake($options, 'daemon') === true){

            //Execution should wait here until we get a command
            $line = fgets(STDIN);

            $payload = null;

            if($type = $protocol->decode($line, $payload)){

                if(!$payload instanceof \stdClass)
                    throw new \Exception('Got Hazaar protocol packet without payload!');

                //Synchronise the timezone with the server
                if($tz = ake($payload, 'timezone'))
                    date_default_timezone_set($tz);

                if($config = ake($payload, 'config'))
                        $application->config->extend($config);

                switch ($type) {

                    case 'EXEC' :

                        $code = null;

                        if(is_array($payload->exec)){

                            $class = new \ReflectionClass($payload->exec[0]);
                
                            $method = $class->getMethod($payload->exec[1]);
                
                            if($method->isStatic() && $method->isPublic()){
                                
                                $file = file($method->getFileName());
                    
                                $start_line = $method->getStartLine() - 1;
                    
                                $end_line = $method->getEndLine();
                    
                                if(preg_match('/function\s+\w+(\(.*)/', $file[$start_line], $matches))
                                    $file[$start_line] = 'function' . $matches[1];
                    
                                if($namespace = $class->getNamespaceName())
                                    $code = "namespace $namespace;\n\n";
                    
                                $code .= '$_function = ' . implode("\n", array_splice($file, $start_line, $end_line - $start_line)) . ';';
                    
                            }

                        }else $code = '$_function = ' . $payload->exec . ';';

                        if(is_string($code)){

                            $container = new \Hazaar\Warlock\Container($application, $protocol);

                            $exitcode = $container->exec($code, ake($payload, 'params'));

                        }elseif($class instanceof \ReflectionClass 
                            && $method instanceof \ReflectionMethod
                            && $class->isInstantiable()
                            && $class->isSubclassOf('Hazaar\\Warlock\\Process')
                            && $method->isPublic()
                            && !$method->isStatic()){

                            $process = $class->newInstance($application, $protocol);

                            if($class->isSubclassOf('Hazaar\\Warlock\\Service'))
                                $process->state = HAZAAR_SERVICE_RUNNING;

                            $exitcode = $method->invokeArgs($process, ake($payload, 'params', array()));

                        }else throw new \Exception('Method can not be executed.');

                        break;

                    case 'SERVICE' :

                        if(!property_exists($payload, 'name')) {

                            $exitcode = 3;

                            break;

                        }

                        $service = self::getServiceClass($payload->name, $application, $protocol, false);

                        if($service instanceof \Hazaar\Warlock\Service){

                            $exitcode = call_user_func(array($service, 'main'), ake($payload, 'params'), ake($payload, 'dynamic', false));

                        } else {

                            $exitcode = 3;

                        }

                        break;

                    default:

                        $exitcode = 2;

                        break;

                }

            }

        }elseif(is_string($service_name)){

            $service = self::getServiceClass($service_name, $application, $protocol, true);

            if(!$service instanceof \Hazaar\Warlock\Service)
                die("Could not find service named '$service_name'.\n");

            $exitcode = call_user_func(array($service, 'main'));

        }else{

            $exitcode = self::showHelp();

        }

        return $exitcode;

    }

    static public function getServiceClass($service_name, \Hazaar\Application $application, \Hazaar\Warlock\Protocol $protocol, $remote = false){

        $class_search = array(
            'Application\\Service\\' . ucfirst($service_name),
            ucfirst($service_name) . 'Service'
        );

        $service = null;

        foreach($class_search as $service_class){

            if(!class_exists($service_class))
                continue;

            $service = new $service_class($application, $protocol, $remote);

        }

        return $service;

    }

}
