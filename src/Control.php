<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

/**
 * @brief       Control class for Warlock
 *
 * @detail      This class creates a connection to the Warlock server from within a Hazaar application allowing the
 *              application to send triggers or schedule jobs for delayed execution.
 *
 * @since       2.0.0
 *
 * @module      warlock
 */
class Control extends Process {

    public  $config;

    private $cmd;

    private $outputfile;

    private $pidfile;

    private $server_pid;

    function __construct($autostart = NULL) {

        if(! extension_loaded('sockets'))
            throw new \Exception('The sockets extension is not loaded.');

        $app = \Hazaar\Application::getInstance();

        $guid_file = $app->runtimePath('warlock.guid');

        if(!file_exists($guid_file)
            || ($this->id = file_get_contents($guid_file)) == FALSE) {

            $this->id = guid();

            file_put_contents($guid_file, $this->id);

        }

        $defaults = array(
            'sys'      => array(
                'id'        => crc32(APPLICATION_PATH),
                'autostart' => FALSE,
                'pid'       => 'warlock.pid'
            ),
            'server'   => array(
                'listen'  => '127.0.0.1',
                'port'    => 8000,
                'encoded' => TRUE
            ),
            'timeouts' => array(
                'connect'   => 5,
                'subscribe' => 60
            ),
            'admin'    => array(
                'trigger' => 'warlockadmintrigger',
                'key'     => '0000'
            ),
            'log'      => array(
                'file'  => 'warlock.log',
                'error' => 'warlock-error.log',
                'rrd'   => 'warlock.rrd'
            )
        );

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, $defaults);

        $app = \Hazaar\Application::getInstance();

        $this->outputfile = $app->runtimePath($this->config->log->file);

        $this->pidfile = $app->runtimePath($this->config->sys->pid);

        $protocol = new \Hazaar\Application\Protocol($this->config->sys->id, $this->config->server->encoded);

        parent::__construct($app, $protocol);

        /**
         * First we check to see if we need to start the Warlock server process
         */
        if($autostart === NULL)
            $autostart = (boolean)$this->config->sys->autostart;

        if($this->isRunning()) {

            if(! $this->connect(APPLICATION_NAME, $this->config->server['port'])) {

                $this->disconnect(FALSE);

                throw new \Exception('Warlock is already running but we were unable to communicate with it.');

            }

            $this->send('sync', array('admin_key' => $this->config->admin->key));

            if($this->recv() !== 'OK')
                throw new \Exception('Connected to Warlock, but server refused our admin key!');

        } elseif($autostart) {

            if($this->start()) {

                if(! $this->connect()) {

                    $this->disconnect(FALSE);

                    throw new \Exception('Warlock was automatically started but we are unable to communicate with it.');

                }

            } else {

                throw new \Exception('Autostart of Warlock server has failed!');

            }

        }

    }

    public function isRunning() {

        if(!file_exists($this->pidfile))
            return false;

        if(!($pid = (int)file_get_contents($this->pidfile)))
            return false;

        if(substr(PHP_OS, 0, 3) == 'WIN'){

            //Uses windows "tasklist" command to look for $pid (FI PID eq) and output in CSV format (FO CSV) with no header (NH).
            exec('tasklist /FI "PID eq ' . $pid . '" /FO CSV /NH', $tasklist, $return_var);

            if($return_var !== 0 || count($tasklist) < 1)
                return false;

            $parts = str_getcsv($tasklist[0]);

            if(count($parts) <= 1) //A non-CSV response was probably returned.  like a "not found" info line
                return false;

            return ($parts[1] == $pid && strpos(strtolower($parts[0]), 'php') !== false);

        }

        if(file_exists('/proc/' . $pid))
            return (($this->server_pid = $pid) > 0);

        return false;

    }

    public function start($timeout = NULL) {

        if($this->isRunning())
            return true;

        $php_binary = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) == 'WIN')?'.exe':'');

        if(! file_exists($php_binary))
            throw new \Exception('The PHP CLI binary does not exist at ' . $php_binary);

        if(! is_executable($php_binary))
            throw new \Exception('The PHP CLI binary exists but is not executable!');

        $server = dirname(__FILE__) . DIRECTORY_SEPARATOR . 'Server.php';

        if(!file_exists($server))
            throw new \Exception('Warlock server script could not be found!');

        if(substr(PHP_OS, 0, 3) == 'WIN')
            $this->cmd = 'start /B "warlock" "' . $php_binary . '" "' . $server . '"';
        else
            $this->cmd = $php_binary . ' "' . $server . '"';

        $env = array(
            'APPLICATION_PATH' => APPLICATION_PATH,
            'APPLICATION_ENV' => APPLICATION_ENV,
            'WARLOCK_EXEC' => 1,
            'WARLOCK_OUTPUT' => 'file'
        );

        foreach($env as $name => $value)
            putenv($name . '=' . $value);

        //Start the server.  This should work on Linux and Windows
        pclose(popen($this->cmd, 'r'));

        //The above replaces this:

        /*
        $this->server_pid = (int)exec(implode(' ', $env) . ' ' . sprintf("%s >> %s 2>&1 & echo $!", $this->cmd, $this->outputfile));
        if(! $this->server_pid > 0)
        return FALSE;
         */

        $start_check = time();

        if(! $timeout)
            $timeout = $this->config->timeouts->connect;

        while(! $this->isRunning()) {

            if(time() > ($start_check + $timeout))
                return FALSE;

            usleep(100);

        }

        return TRUE;

    }

    public function stop() {

        if($this->isRunning()) {

            $this->send('shutdown');

            if($this->recv($packet) == 'OK') {

                $this->disconnect();

                return TRUE;

            }

        }

        return FALSE;

    }

    public function status() {

        if($this->isRunning()) {

            $this->send('status');

            if($this->recv($packet) == 'STATUS')
                return $packet;

        }

        $buf = array(
            'state'       => 'stopped',
            'pid'         => 'none',
            'started'     => 0,
            'uptime'      => 0,
            'connections' => 0,
            'stats'       => array(
                'processed' => 0,
                'processes' => 0,
                'execs'     => 0,
                'failed'    => 0,
                'queue'     => 0,
                'retries'   => 0,
                'lateExecs' => 0,
                'limitHits' => 0,
                'waiting'   => 0,
                'triggers'  => 0
            ),
            'clients'     => array(),
            'queue'       => array(),
            'processes'   => array(),
            'services'    => array(),
            'triggers'    => array()
        );

        return $buf;

    }

    public function runDelay($delay, \Closure $code, $params = NULL, $tag = NULL, $overwrite = FALSE) {

        $function = new \Hazaar\Closure($code);

        $data = array(
            'application' => array(
                'path' => APPLICATION_PATH,
                'env'  => APPLICATION_ENV
            ),
            'value'       => $delay,
            'function'    => array(
                'code' => (string)$function
            )
        );

        if($tag) {

            $data['tag'] = $tag;

            $data['overwrite'] = strbool($overwrite);

        }

        if(! is_array($params))
            $params = array($params);

        $data['function']['params'] = $params;

        $this->send('delay', $data);

        if($this->recv($payload) == 'OK')
            return $payload['job_id'];

        return FALSE;

    }

    public function schedule($when, \Closure $code, $params = NULL, $tag = NULL, $overwrite = FALSE) {

        $function = new \Hazaar\Closure($code);

        $data = array(
            'application' => array(
                'path' => APPLICATION_PATH,
                'env'  => APPLICATION_ENV
            ),
            'when'        => $when,
            'function'    => array(
                'code' => (string)$function
            )
        );

        if($tag) {

            $data['tag'] = $tag;

            $data['overwrite'] = strbool($overwrite);

        }

        if(is_array($params)) {

            $data['function']['params'] = $params;

        }

        $this->send('schedule', $data);

        if($this->recv($payload) == 'OK')
            return $payload['job_id'];

        return FALSE;

    }

    public function cancel($job_id) {

        $this->send('cancel', $job_id);

        return ($this->recv() == 'OK');

    }

    public function subscribe($event, $filter = NULL) {

        $subscribe = array(
            'id'     => $event,
            'filter' => $filter
        );

        if(array_key_exists('REMOTE_ADDR', $_SERVER))
            $subscribe['client_ip'] = $_SERVER['REMOTE_ADDR'];

        if(array_key_exists('REMOTE_USER', $_SERVER))
            $subscribe['client_user'] = $_SERVER['REMOTE_USER'];

        $this->send('subscribe', $subscribe);

        return ($this->recv() == 'OK');

    }

    public function trigger($event, $data = NULL) {

        $packet = array(
            'id' => $event
        );

        if($data)
            $packet['data'] = $data;

        $this->send('trigger', $packet);

        return ($this->recv() == 'OK');

    }

    public function startService($name) {

        $this->send('enable', $name);

        return ($this->recv() == 'OK');

    }

    public function stopService($name) {

        $this->send('disable', $name);

        return ($this->recv() == 'OK');

    }

    public function ping() {

        $this->send('ping');

        $start = microtime(true);

        if($this->recv($payload) == 'PONG')
            return (microtime(true) - $start);

        return false;

    }

}

