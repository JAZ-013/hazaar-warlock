<?php

namespace Hazaar\Warlock;

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
abstract class Service extends Process implements ServiceInterface {

    protected $config;

    private   $name;

    protected $options       = array();

    private   $subscriptions = array();

    private   $schedule      = array(); //callback execution schedule

    private   $next          = NULL;

    final function __constructdkjasdkjashd($application, $protocol = NULL) {

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

    protected function processCommand($command, $payload = NULL) {

        switch($command) {

            case 'CANCEL':

                return $this->stop();

        }

        return parent::processCommand($command, $payload);

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

        return $this->state = HAZAAR_SERVICE_STOPPING;

    }

    public function restart() {

        $this->stop();

        return $this->start();

    }

    public function state() {

        return $this->state;

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


