<?php

namespace Hazaar\Warlock;

require('Functions.php');

define('W_LOCAL', -1);

/**
 * @brief       The Warlock application service class
 *
 * @detail      Services are long running processes that allow code to be executed on the server in the background
 *              without affecting or requiring any interaction with the front-end. Services are managed by the Warlock
 *              process and can be set to start when Warlock starts or enabled/disabled manually using the
 *              Hazaar\Warlock\Control class.
 *
 *              Services are executed within the Application context and therefore have access to everything (configs,
 *              classes/models, cache, etc) that your application front-end does.
 *
 *              See the "Services Documentation":http://www.hazaarmvc.com/docs/advanced-features/warlock/services for
 *              information on how to write and manage services.
 *
 * @since       2.0.0
 *
 * @module      warlock
 */
abstract class Service extends Process {

    protected $name;

    protected $config;

    protected $state    = HAZAAR_SERVICE_INIT;

    protected $schedule = array();              //callback execution schedule

    protected $next     = null;                 //Timestamp of next executable schedule item

    protected $slept    = false;

    private   $last_heartbeat;

    private   $last_checkfile;

    private   $service_file;                    //The file in which the service is defined

    private   $service_file_mtime;              //The last modified time of the service file

    private   $__log_levels = array();

    private   $__str_pad = 0;

    private   $__log_file;

    private   $__log;
    
    private   $__local_log_level = W_INFO;

    private   $__remote = false;

    final function __construct(\Hazaar\Application $application, Protocol $protocol, $remote = false) {

        $this->start = time();

        $class = get_class($this);

        if((substr($class, -7) === 'Service')){

            $this->name = strtolower(substr($class, 0, strlen($class) - 7));

        }else{

            $parts = explode('\\', $class);

            $this->name = strtolower(array_pop($parts));

        }

        Config::$default_config['sys']['id'] = crc32(APPLICATION_PATH);

        Config::$default_config['sys']['application_name'] = APPLICATION_NAME;

        $warlock = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, Config::$default_config);

        $this->log(W_LOCAL, 'Loaded config for ' . APPLICATION_ENV);

        $defaults = array(
            $this->name => array(
                'enabled'   => false,
                'heartbeat' => 60,
                'checkfile' => 1,
                'connect_retries' => 3,        //When establishing a control channel, make no more than this number of attempts before giving up
                'connect_retry_delay' => 100,  //When making multiple attempts to establish the control channel, wait this long between each
                'server' => array(
                    'host' => '127.0.0.1',
                    'port' => $warlock->server['port'],
                    'access_key' => $warlock->admin->get('key')
                ),
                'silent' => false,
                'application_name' => $warlock->sys['application_name'],
                'log' => $warlock->log
            )
        );

        $config = new \Hazaar\Application\Config('service', APPLICATION_ENV, $defaults);

        $this->config = ake($config, $this->name);

        $consts = get_defined_constants(TRUE);

        //Load the warlock log levels into an array.
        foreach($consts['user'] as $name => $value) {

            if (substr($name, 0, 2) == 'W_'){

                $len = strlen($this->__log_levels[$value] = substr($name, 2));

                if($len > $this->__str_pad)
                    $this->__str_pad = $len;

            }

        }

        $this->__remote = $remote;

        if($this->config->log->has('level') && defined($out_level = $this->config->log->get('level')))
            $this->__local_log_level = constant($out_level);

        if($remote === true && !$this->config->has('server'))
            throw new \Exception("Warlock server required to run in remote service mode.\n");

        $this->__log_file = $warlock->sys['runtimepath'] . DIRECTORY_SEPARATOR . $this->name . '.log';

        $this->__log = fopen($this->__log_file, 'a');

        $this->log(W_LOCAL, "Service '{$this->name}' starting up");

        if(!$application->request instanceof \Hazaar\Application\Request\Http){

            $this->setErrorHandler('__errorHandler');

            $this->setExceptionHandler('__exceptionHandler');

        }

        if($tz = $this->config->get('timezone'))
            date_default_timezone_set($tz);

        if($this->config['checkfile'] > 0){

            $reflection = new \ReflectionClass($this);

            $this->service_file = $reflection->getFileName();

            $this->service_file_mtime = filemtime($this->service_file);

            $this->last_checkfile = time();

        }

        parent::__construct($application, $protocol, getmypid());

        if(method_exists($this, 'construct'))
            $this->construct($this->application);
            
    }

    function __destruct(){

        if($this->__log)
            fclose($this->__log);

        parent::__destruct();

    }

    protected function connect(\Hazaar\Warlock\Protocol $protocol, $guid = null){

        if($this->__remote === true){

            if(!$this->config->has('server'))
                die("Warlock server required to run in remote service mode.\n");

            $headers = array();

            $headers['X-WARLOCK-ACCESS-KEY'] = base64_encode($this->config->server['access_key']);

            $headers['X-WARLOCK-CLIENT-TYPE'] = 'service';

            $conn = new Connection\Socket($protocol);

            $this->log(W_LOCAL, 'Connecting to Warlock server at ' . $this->config->server['host'] . ':' . $this->config->server['port']);

            if(!$conn->connect($this->config['application_name'], $this->config->server['host'], $this->config->server['port'], $headers))
                return false;

            if(($type = $conn->recv($payload)) === false || $type !== 'OK')
                return false;

        }else{

            $conn = new Connection\Pipe($protocol);

        }

        return $conn;

    }

    public function log($level, $message, $name = null){

        if(!is_resource($this->__log))
            return false;

        if($name === null)
            $name = $this->name;

        if($level <= $this->__local_log_level){

            $label = ake($this->__log_levels, $level, 'NONE');

            if(!is_array($message))
                $message = array($message);

            foreach($message as $m){

                $msg = date('Y-m-d H:i:s') . " - $this->name - " . str_pad($label, $this->__str_pad, ' ', STR_PAD_LEFT) . ' - ' . $m . "\n";

                fwrite($this->__log, $msg);

                if($this->__remote === true && $this->config->silent !== true)
                    echo $msg;

            }

            fflush($this->__log);

        }

        return ($level === W_LOCAL) ? true : parent::log($level, $message, $name);

    }

    public function debug($message, $name = null){

        if($name === null)
            $name = $this->name;

        return $this->conn->send('DEBUG', $message, $name);

    }

    private function invokeMethod($method, $arguments = null){

        $args = array();

        $initMethod = new \ReflectionMethod($this, $method);

        foreach($initMethod->getParameters() as $parameter){

            if(!($value = ake($arguments, $parameter->getName())))
                $value = $parameter->getDefaultValue();

            $args[$parameter->getPosition()] = $value;

        }

        return $initMethod->invokeArgs($this, $args);

    }

    final public function main($params = array(), $dynamic = false) {

        $this->log(W_LOCAL, "Service started");

        if($this->config->log->rotate === true){

            $when = ake($this->config->log, 'rotateAt', '0 0 * * * *');

            $logfiles = ake($this->config->log, 'logfiles');

            $this->log(W_LOCAL, "Log rotation is enabled. WHEN=$when LOGFILES=$logfiles");

            $this->cron($when, '__rotateLogFiles', [$logfiles]);

        }

        $init = true;

        if(method_exists($this, 'init'))
            $init = $this->init();

        if($this->state === HAZAAR_SERVICE_INIT) {

            $this->state = (($init === FALSE) ? HAZAAR_SERVICE_ERROR : HAZAAR_SERVICE_READY);

            if($this->state !== HAZAAR_SERVICE_READY)
                return 1;

            $this->state = HAZAAR_SERVICE_RUNNING;

        }

        if($dynamic === true){

            if(!method_exists($this, 'runOnce'))
                return 5;

            if($this->invokeMethod('runOnce', $params) === false)
                return 1;

            return 0;

        }

        if(! $this->start())
            return 1;

        $this->__sendHeartbeat();

        $this->__processSchedule();

        $code = 0;

        while($this->state == HAZAAR_SERVICE_RUNNING || $this->state == HAZAAR_SERVICE_SLEEP) {

            $this->slept = FALSE;

            $this->state = HAZAAR_SERVICE_RUNNING;

            try{

                $ret = $this->invokeMethod('run', $params);

                if($ret === false)
                    $this->state = HAZAAR_SERVICE_STOPPING;

                /*
                * If sleep was not executed in the last call to run(), then execute it now.  This protects bad services
                * from not sleeping as the sleep() call is where new signals are processed.
                */
                if(!$this->slept)
                    $this->sleep(0);

                if($this->service_file_mtime > 0 && time() >= ($this->last_checkfile + $this->config['checkfile'])){

                    $this->last_checkfile = time();

                    clearstatcache(true, $this->service_file);

                    //Check if the service file has been modified and initiate a restart
                    if(filemtime($this->service_file) > $this->service_file_mtime){

                        $this->log(W_INFO, 'Service file modified. Initiating restart.');

                        $this->state = HAZAAR_SERVICE_STOPPING;

                        $code = 6;

                    }

                }

            }
            catch(\Throwable $e){

                $this->__exceptionHandler($e);

                $this->state = HAZAAR_SERVICE_ERROR;

                $code = 7;

            }

        }

        $this->state = HAZAAR_SERVICE_STOPPING;

        $this->log(W_INFO, 'Service is shutting down');

        $this->shutdown();

        $this->state = HAZAAR_SERVICE_STOPPED;

        return $code;

    }

    final public function __errorHandler($errno , $errstr , $errfile = null, $errline  = null, $errcontext = array()){

        ob_start();

        $msg = "#$errno on line $errline in file $errfile\n"
            . str_repeat('-', 40) . "\n$errstr\n" .  str_repeat('-', 40) . "\n";

        debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS) . "\n";

        $msg .= ob_get_clean();

        $this->log(W_LOCAL, 'ERROR ' . $msg);

        $this->send('ERROR', $msg);

        return true;

    }

    final public function __exceptionHandler($e){

        ob_start();

        $msg = "#{$e->getCode()} on line {$e->getLine()} in file {$e->getFile()}\n"
            . str_repeat('-', 40) . "\n{$e->getMessage()}\n" . str_repeat('-', 40) . "\n";

        debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS) . "\n";

        $msg .= ob_get_clean();

        $this->log(W_LOCAL, 'EXCEPTION ' . $msg);

        $this->send('ERROR', $msg);

        return true;

    }

    final protected function __processCommand($command, $payload = NULL) {

        switch($command) {

            case 'STATUS':

                $this->__sendHeartbeat();

                break;

            case 'CANCEL':

                return $this->stop();

        }

        try {

            return parent::__processCommand($command, $payload);

        }
        catch(\Exception $e){

            $this->__exceptionHandler($e);

        }

        return false;

    }

    final private function __processSchedule() {

        if(! is_array($this->schedule) || ! count($this->schedule) > 0)
            return;

        if(($count = count($this->schedule))> 0)
            $this->log(W_DEBUG, "Processing $count scheduled actions");

        $this->next = NULL;

        foreach($this->schedule as $id => &$exec) {

            if(time() >= $exec['when']) {

                $this->state = HAZAAR_SERVICE_RUNNING;

                if(is_string($exec['callback']))
                    $exec['callback'] = array($this, $exec['callback']);

                try{

                    if(is_callable($exec['callback'])){

                        $this->log(W_DEBUG, "RUN: ACTION=$exec[label]");

                        call_user_func_array($exec['callback'], $exec['args']);

                    }else $this->log(W_ERR, "Scheduled action $exec[label] is not callable!");

                }
                catch(\Exception $e){

                    $this->__exceptionHandler($e);

                }

                switch($exec['type']) {
                    case HAZAAR_SCHEDULE_INTERVAL:

                        if($exec['when'] = time() + $exec['interval'])
                            $this->log(W_DEBUG, "INTERVAL: ACTION=$exec[label] NEXT=" . date('Y-m-d H:i:s', $exec['when']));

                        break;

                    case HAZAAR_SCHEDULE_CRON:

                        if($exec['when'] = $exec['cron']->getNextOccurrence())
                            $this->log(W_DEBUG, "SCHEDULED: ACTION=$exec[label] NEXT=" . date('Y-m-d H:i:s', $exec['when']));

                        break;

                    case HAZAAR_SCHEDULE_DELAY:
                    case HAZAAR_SCHEDULE_NORM:
                    default:

                        unset($this->schedule[$id]);

                        break;

                }

                if($exec['when'] === null
                    || $exec['when'] === 0
                    || ($exec['type'] !== HAZAAR_SCHEDULE_INTERVAL && $exec['when'] < time())){

                    unset($this->schedule[$id]);

                    $this->log(W_DEBUG, "UNSCHEDULED: ACTION=$exec[label]");

                }

            }

            if($this->next === NULL || ($exec['when'] && $exec['when'] < $this->next))
                $this->next = $exec['when'];

        }

        if($this->next)
            $this->log(W_NOTICE, 'Next scheduled action is at ' . date('Y-m-d H:i:s', $this->next));

    }

    final protected function __sendHeartbeat() {

        $status = array(
            'pid'        => getmypid(),
            'name'       => $this->name,
            'start'      => $this->start,
            'state_code' => $this->state,
            'state'      => $this->__stateString($this->state),
            'mem'        => memory_get_usage(),
            'peak'       => memory_get_peak_usage()
        );

        $this->last_heartbeat = time();

        $this->send('status', $status);

        return true;

    }

    final private function __stateString($state = NULL) {

        if($state === NULL)
            $state = $this->state;

        $strings = array(
            HAZAAR_SERVICE_ERROR    => 'Error',
            HAZAAR_SERVICE_INIT     => 'Initializing',
            HAZAAR_SERVICE_READY    => 'Ready',
            HAZAAR_SERVICE_RUNNING  => 'Running',
            HAZAAR_SERVICE_SLEEP    => 'Sleeping',
            HAZAAR_SERVICE_STOPPING => 'Stopping',
            HAZAAR_SERVICE_STOPPED  => 'Stopped'
        );

        return $strings[$state];

    }

    final private function __rotateLogFiles($logfiles = 0){

        $this->log(W_LOCAL, 'ROTATING LOG FILES: MAX=' . $logfiles);

        fclose($this->__log);

        rotateLogFile($this->__log_file, $logfiles);

        $this->__log = fopen($this->__log_file, 'a');

        $this->log(W_LOCAL, 'New log file started');

    }

    /*
     * BUILT-IN PLACEHOLDER METHODS
     */
    public function run() {

        $this->sleep(60);

    }

    public function shutdown() {

        return true;

    }

    /*
     * CONTROL METHODS
     */

    final private function start() {

        if(\Hazaar\Map::is_array($events = $this->config->get('subscribe'))){

            foreach($events as $event_name => $event){

                if(\Hazaar\Map::is_array($event)){

                    if(!($action = ake($event, 'action')))
                        continue;

                    $this->subscribe($event_name, $action, ake($event, 'filter'));

                }else{

                    $this->subscribe($event_name, $event);

                }

            }

        }

        if(\Hazaar\Map::is_array($schedule = $this->config->get('schedule'))){

            foreach($schedule as $item){

                if(!(\Hazaar\Map::is_array($item) && $item->has('action')))
                    continue;

                if($item->has('interval'))
                    $this->interval(ake($item, 'interval'), ake($item, 'action'), ake($item, 'args'));

                if($item->has('delay'))
                    $this->delay(ake($item, 'delay'), ake($item, 'action'), ake($item, 'args'));

                if($item->has('when'))
                    $this->cron(ake($item, 'when'), ake($item, 'action'), ake($item, 'args'));

            }

        }

        return true;

    }

    final public function stop() {

        return $this->state = HAZAAR_SERVICE_STOPPING;

    }

    final public function restart() {

        $this->stop();

        return $this->start();

    }

    final public function state() {

        return $this->state;

    }

    /**
     * Sleep for a number of seconds.  If data is received during the sleep it is processed.  If the timeout is greater
     * than zero and data is received, the remaining timeout amount will be used in subsequent selects to ensure the
     * full sleep period is used.  If the timeout parameter is not set then the loop will just dump out after one
     * execution.
     *
     * @param int $timeout
     */
    final protected function sleep($timeout = 0) {

        $start = microtime(true);

        $slept = FALSE;

        //Sleep if we are still sleeping and the timeout is not reached.  If the timeout is NULL or 0 do this process at least once.
        while($this->state < 4 && ($slept === FALSE || ($start + $timeout) >= microtime(true))) {

            $tv_sec = 0;

            $tv_usec = 0;

            if($timeout > 0) {

                $this->state = HAZAAR_SERVICE_SLEEP;

                $diff = ($start + $timeout) - microtime(true);

                $hb = $this->last_heartbeat + $this->config['heartbeat'];

                $next = ((! $this->next || $hb < $this->next) ? $hb : $this->next);

                if($next != NULL && $next < ($diff + time()))
                    $diff = $next - time();

                if($diff > 0) {

                    $tv_sec = floor($diff);

                    $tv_usec = round(($diff - floor($diff)) * 1000000);

                } else {

                    $tv_sec = 1;

                }

            }

            $payload = null;

            if($type = $this->recv($payload, $tv_sec, $tv_usec))
                $this->__processCommand($type, $payload);

            if($this->next > 0 && $this->next <= time())
                $this->__processSchedule();

            if(($this->last_heartbeat + $this->config['heartbeat']) <= time())
                $this->__sendHeartbeat();

            $slept = true;

        }

        $this->slept = true;

        return true;

    }

    /*
     * Command scheduling
     */
    final public function delay($seconds, $callback, $arguments = array()) {

        if(!is_int($seconds))
            return false;

        if(!is_callable($callback) && !method_exists($this, $callback))
            return false;

        if(!is_array($arguments))
            $arguments = ($arguments instanceof \Hazaar\Map) ? $arguments->toArray() : array($arguments);

        $id = uniqid();

        $label = (is_string($callback) ? $callback : '<func>');

        $when = time() + $seconds;

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_DELAY,
            'label'    => $label,
            'when'     => $when,
            'callback' => $callback,
            'args'   => $arguments
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        $this->log(W_DEBUG, "SCHEDULED: ACTION=$label DELAY=$seconds NEXT=" . date('Y-m-d H:i:s', $when));

        return $id;

    }

    final public function interval($seconds, $callback, $arguments = array()) {

        if(!is_int($seconds))
            return false;

        if(!is_callable($callback) && !method_exists($this, $callback))
            return false;

        if(!is_array($arguments))
            $arguments = ($arguments instanceof \Hazaar\Map) ? $arguments->toArray() : array($arguments);

        $id = uniqid();

        $label = (is_string($callback) ? $callback : '<func>');

        //First execution in $seconds
        $when = time() + $seconds;

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_INTERVAL,
            'label'    => $label,
            'when'     => $when,
            'interval' => $seconds,
            'callback' => $callback,
            'args'   => $arguments
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        $this->log(W_DEBUG, "SCHEDULED: ACTION=$label INTERVAL=$seconds NEXT=" . date('Y-m-d H:i:s', $when));

        return $id;

    }

    final public function schedule($date, $callback, $arguments = array()) {

        if(!is_callable($callback) && !method_exists($this, $callback))
            return false;

        if(!is_array($arguments))
            $arguments = ($arguments instanceof \Hazaar\Map) ? $arguments->toArray() : array($arguments);

        if(! $date instanceof \Hazaar\Date)
            $date = new \Hazaar\Date($date);

        if($date->getTimestamp() <= time())
            return FALSE;

        $id = uniqid();

        $label = (is_string($callback) ? $callback : '<func>');

        $when = $date->getTimestamp();

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_NORM,
            'label'    => $label,
            'when'     => $when,
            'callback' => $callback,
            'args'   => $arguments
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        $this->log(W_DEBUG, "SCHEDULED: ACTION=$label SCHEDULE=$date NEXT=" . date('Y-m-d H:i:s', $when));

        return $id;

    }

    final public function cron($format, $callback, $arguments = array()) {

        if(!is_callable($callback) && !method_exists($this, $callback))
            return false;

        if(!is_array($arguments))
            $arguments = ($arguments instanceof \Hazaar\Map) ? $arguments->toArray() : array($arguments);

        $id = uniqid();

        $label = (is_string($callback) ? $callback : '<func>');

        $cron = new \Hazaar\Cron($format);

        $when = $cron->getNextOccurrence();

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_CRON,
            'label'    => $label,
            'when'     => $when,
            'callback' => $callback,
            'args'   => $arguments,
            'cron'     => $cron
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        $this->log(W_DEBUG, "SCHEDULED: ACTION=$label CRON=\"$format\" NEXT=" . date('Y-m-d H:i:s', $when));

        return $id;

    }

    final public function cancel($id) {

        if(! array_key_exists($id, $this->schedule))
            return FALSE;

        unset($this->schedule[$id]);

        return true;

    }

    final public function signal($event_id, $data){

        return $this->send('SIGNAL', array('service' => $this->name, 'id' => $event_id, 'data' => $data));

    }

    final public function send($command, $payload = null){

        $result = parent::send($command, $payload);

        if($result === false){

            $this->log(W_LOCAL, 'An error occured while sending command.  Stopping.');

            $this->stop();

        }

        return $result;

    }

    final public function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0){

        $result = parent::recv($payload, $tv_sec, $tv_usec);

        if($result === false){

            $this->log(W_LOCAL, 'An error occured while receiving data.  Stopping.');

            $this->stop();

        }

        return $result;

    }

}

