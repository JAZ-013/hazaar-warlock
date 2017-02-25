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

abstract class Process {

    protected $application;

    private   $socket;

    private   $state         = HAZAAR_SERVICE_INIT;

    private   $slept         = FALSE;

    protected $options       = array();

    private   $protocol;

    private   $next          = NULL;

    final function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol) {

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

    }

    public function connect($port, $job_id, $access_key){

        if(!($this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP)))
            throw new \Exception('Unable to create TCP socket!');

        if(!socket_connect($this->socket, '127.0.0.1', $port))
            throw new \Exception('Unable to connecto to localhost:' . $port);

        //TODO: Initiate the WebSockets handshake!

        /*$this->send('sync', array(
            'job_id' => $job_id,
            'access_key' => $access_key,
            'process_id' => getmypid(),
            'user' => base64_encode(get_current_user())
        ));*/

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

        if(!$this->socket)
            return false;

        $packet = $this->protocol->encode($command, $payload);

        if(!socket_write($this->socket, $packet)){

            socket_close($this->socket);

            throw new \Exception('Socket write error!');

        }

        return true;

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

        if(!$this->socket)
            throw new \Exception('Trying to sleep without a socket!');

        $start = microtime(true);

        $read = array(
            $this->socket
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

            if(socket_select($read, $null, $null, $tv_sec, $tv_usec) > 0) {

                $payload = NULL;

                $blksize = unpack('N', socket_read($this->socket, 4));

                dump($blksize);

                $buffer = socket_read($this->socket, $blksize);

                if($type = $this->protocol->decode($buffer, $payload))
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

    protected function processCommand($command, $payload = NULL) {

        switch($command) {

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

            case 'STATUS':

                $this->sendHeartbeat();

                break;

            default:

                $this->send('DEBUG', 'Unhandled command: ' . $command);

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

}
