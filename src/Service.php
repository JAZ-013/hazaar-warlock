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
abstract class Service implements ServiceInterface {

    protected $config;

    protected $application;

    private   $name;

    private   $stdin         = NULL;

    private   $start         = NULL;

    private   $state         = HAZAAR_SERVICE_INIT;

    private   $slept         = FALSE;

    protected $options       = array();

    private   $protocol;

    private   $subscriptions = array();

    private   $schedule      = array(); //callback execution schedule

    private   $next          = NULL;

    private   $lastHeartbeat = 0;

    final function __construct($application, $protocol = NULL) {

        $this->application = $application;

        $this->start = time();

        if(preg_match('/^(\w*)Service$/', get_class($this), $matches)) {

            $name = $matches[1];

        } else {

            throw new \Exception('Invalid service name ' . get_class($this));

        }

        $this->name = $name;

        $this->stdin = fopen('php://stdin', 'r');

        stream_set_blocking($this->stdin, true);

        $this->protocol = $protocol;

        $defaults = array(
            $name => array(
                'enabled'   => true,
                'heartbeat' => 60
            )
        );

        $config = new \Hazaar\Application\Config('service', APPLICATION_ENV, $defaults);

        $this->config = ake($config, $name);

        $admin_key = getenv('HAZAAR_ADMIN_KEY');

        $this->send('sync', array('client_id' => guid(), 'user' => base64_encode(get_current_user()), 'admin_key' => $admin_key));

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

    public function send($command, $payload = NULL) {

        if(! $this->protocol)
            throw new \Exception('No protocol object.  How can I talk without it!?!?');

        echo $this->protocol->encode($command, $payload) . "\n";

        $this->sleep();

        return true;

    }

    public function main() {

        if(! $this->start())
            return 1;

        $this->sendHeartbeat();

        $this->processSchedule();

        while($this->state == HAZAAR_SERVICE_RUNNING || $this->state == HAZAAR_SERVICE_SLEEP) {

            $this->slept = FALSE;

            $this->state = HAZAAR_SERVICE_RUNNING;

            $ret = $this->run();

            if($ret === false)
                $this->state = HAZAAR_SERVICE_STOPPING;

            if(($this->lastHeartbeat + $this->config['heartbeat']) <= time())
                $this->sendHeartbeat();

            /*
             * If sleep was not executed in the last call to run(), then execute it now.  This protects bad services
             * from not sleeping as the sleep() call is where new signals are processed.
             */
            if(! $this->slept)
                $this->sleep(0);

        }

        $this->state = HAZAAR_SERVICE_STOPPING;

        $this->shutdown();

        //Do a sleep so that we can correctly flush any output that may have been sent before we exit.
        while(ob_get_length() > 0)
            $this->sleep();

        $this->state = HAZAAR_SERVICE_STOPPED;

        return 0;

    }

    /**
     * Sleep for a number of seconds.  If data is received during the sleep it is processed.  If the timeout is greater
     * than zero and data is received, the remaining timeout amount will be used in subsequent selects to ensure the
     * full sleep period is used.  If the timeout parameter is not set then the loop will just dump out after one
     * execution.
     *
     * @param int $timeout
     */
    protected function sleep($timeout = 0) {

        if(! $this->protocol)
            return FALSE;

        $start = microtime(true);

        $read = array(
            STDIN
        );

        $null = NULL;

        $slept = FALSE;

        //Sleep if we are still sleeping and the timeout is not reached.  If the timeout is NULL or 0 do this process at least once.
        while($this->state < 4 && ($slept === FALSE || ($start + $timeout) >= microtime(true))) {

            $tv_sec = 0;

            $tv_usec = 0;

            if($timeout > 0) {

                $this->state = HAZAAR_SERVICE_SLEEP;

                $diff = ($start + $timeout) - microtime(true);

                $hb = $this->lastHeartbeat + $this->config['heartbeat'];

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

            if(stream_select($read, $null, $null, $tv_sec, $tv_usec) > 0) {

                $payload = NULL;

                if($type = $this->protocol->decode(fgets(STDIN), $payload))
                    $this->processCommand($type, $payload);

            }

            if($this->next > 0 && $this->next <= time())
                $this->processSchedule();

            if(($this->lastHeartbeat + $this->config['heartbeat']) <= time())
                $this->sendHeartbeat();

            $slept = true;

        }

        $this->slept = true;

        return true;

    }

    private function processCommand($command, $payload = NULL) {

        $type_name = $this->protocol->getTypeName($command);

        switch($type_name) {

            case 'EVENT':

                if(! (array_key_exists('id', $payload) && array_key_exists($payload['id'], $this->subscriptions)))
                    return FALSE;

                try {

                    call_user_func_array(array($this, $this->subscriptions[$payload['id']]), array($payload));

                }
                catch(\Exception $e) {

                    error_log('ERROR: ' . $e->getMessage());

                }

                break;

            case 'CANCEL':

                $this->stop();

                break;

            case 'STATUS':

                $this->sendHeartbeat();

                break;

        }

        return true;

    }

    private function sendHeartbeat() {

        $status = array(
            'pid'        => getmypid(),
            'name'       => $this->name,
            'start'      => $this->start,
            'state_code' => $this->state,
            'state'      => $this->stateString($this->state),
            'mem'        => memory_get_usage(),
            'peak'       => memory_get_peak_usage()
        );

        $this->lastHeartbeat = time();

        $this->send('status', $status);

        return true;

    }

    private function processSchedule() {

        if(! is_array($this->schedule) || ! count($this->schedule) > 0)
            return;

        $this->next = NULL;

        foreach($this->schedule as $id => &$exec) {

            if(time() >= $exec['when']) {

                $this->state = HAZAAR_SERVICE_RUNNING;

                call_user_func_array(array($this, $exec['callback']), $exec['params']);

                switch($exec['type']) {
                    case HAZAAR_SCHEDULE_INTERVAL:

                        $this->next = $exec['when'] = $exec['when'] + $exec['interval'];

                        break;

                    case HAZAAR_SCHEDULE_CRON:

                        $this->next = $exec['when'] = $exec['cron']->getNextOccurrence($exec['when'] + 60);

                        break;

                    case HAZAAR_SCHEDULE_DELAY:
                    case HAZAAR_SCHEDULE_NORM:
                    default:

                        unset($this->schedule[$id]);

                        break;

                }

            } elseif($this->next === NULL || $exec['when'] < $this->next) {

                $this->next = $exec['when'];

            }

        }

    }

    /*
     * BUILT-IN PLACEHOLDER METHODS
     */
    public function init() {

        return true;

    }

    public function run() {

        $this->sleep(60);

    }

    public function shutdown() {

        return true;

    }

    /*
     * CONTROL METHODS
     */

    public function start() {

        $init = $this->init($this->config);

        if($this->state === HAZAAR_SERVICE_INIT) {

            $this->state = (($init === FALSE) ? HAZAAR_SERVICE_ERROR : HAZAAR_SERVICE_READY);

            if($this->state != HAZAAR_SERVICE_READY)
                return FALSE;

            $this->state = HAZAAR_SERVICE_RUNNING;

        }

        return true;

    }

    public function stop() {

        $this->state = HAZAAR_SERVICE_STOPPING;

    }

    public function restart() {

        $this->stop();

        $this->start();

    }

    public function state() {

        return $this->state;

    }

    public function stateString($state = NULL) {

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

    /*
     * COMMUNICATIONS
     */
    protected function subscribe($event, $callback, $filter = NULL) {

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

    /*
     * Command scheduling
     */
    protected function delay($seconds, $callback, $params = array()) {

        $id = uniqid();

        $when = time() + $seconds;

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_DELAY,
            'when'     => $when,
            'callback' => $callback,
            'params'   => $params
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        return $id;

    }

    protected function interval($seconds, $callback, $params = array()) {

        $id = uniqid();

        //First execution in $seconds
        $when = time() + $seconds;

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_INTERVAL,
            'when'     => $when,
            'interval' => $seconds,
            'callback' => $callback,
            'params'   => $params
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        return $id;

    }

    protected function schedule($date, $callback, $params = array()) {

        if(! $date instanceof \Hazaar\Date)
            $date = new \Hazaar\Date($date);

        if($date->getTimestamp() <= time())
            return FALSE;

        $id = uniqid();

        $when = $date->getTimestamp();

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_NORM,
            'when'     => $when,
            'callback' => $callback,
            'params'   => $params
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        return $id;

    }

    protected function cron($format, $callback, $params = array()) {

        $id = uniqid();

        $cron = new \Hazaar\Cron($format);

        $when = $cron->getNextOccurrence();

        $this->schedule[$id] = array(
            'type'     => HAZAAR_SCHEDULE_CRON,
            'when'     => $when,
            'callback' => $callback,
            'params'   => $params,
            'cron'     => $cron
        );

        if($this->next === NULL || $when < $this->next)
            $this->next = $when;

        return $id;

    }

    protected function cancel($id) {

        if(! array_key_exists($id, $this->schedule))
            return FALSE;

        unset($this->schedule[$id]);

        return true;

    }

}
