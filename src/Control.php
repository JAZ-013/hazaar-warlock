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

    private $pidfile;

    private $server_pid;

    static private $guid;

    static private $instance = array();

    function __construct($autostart = NULL, $config = null, $instance_key = null, $require_connect = true) {

        if(! extension_loaded('sockets'))
            throw new \Exception('The sockets extension is not loaded.');

        Config::$default_config['sys']['id'] = crc32(APPLICATION_PATH);

        Config::$default_config['sys']['application_name'] = APPLICATION_NAME;

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, Config::$default_config);

        if(!$this->config->loaded())
            throw new \Exception('There is no warlock configuration file.  Warlock is disabled!');

        if($config)
            $this->config->extend($config);

        if($this->config->client['port'] === null)
            $this->config->client['port'] = $this->config->server['port'];

        if($this->config->client['server'] === null){

            if(trim($this->config->server['listen']) == '0.0.0.0')
                $this->config->client['server'] = '127.0.0.1';
            else
                $this->config->client['server'] = $this->config->server['listen'];

        }

        if(!$instance_key)
            $instance_key = hash('crc32b', $this->config->client['server'] . $this->config->client['port']);

        if(array_key_exists($instance_key, Control::$instance))
            throw new \Exception('There is already a control instance for this server:host.  Please use ' . __CLASS__ . '::getInstance()');

        Control::$instance[$instance_key] = $this;

        $app = \Hazaar\Application::getInstance();

        $protocol = new \Hazaar\Application\Protocol($this->config->sys->id, $this->config->server->encoded);

        parent::__construct($app, $protocol, Control::$guid);

        if(!Control::$guid){

            $guid_file = $app->runtimePath('warlock.guid');

            if(!file_exists($guid_file) || (Control::$guid = file_get_contents($guid_file)) == FALSE) {

                Control::$guid = guid();

                file_put_contents($guid_file, Control::$guid);

            }

            /**
             * First we check to see if we need to start the Warlock server process
             */
            if($autostart === NULL)
                $autostart = (boolean)$this->config->sys->autostart;

            if($autostart === true){

                if(!$this->config->sys['php_binary'])
                    $this->config->sys['php_binary'] = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) == 'WIN')?'.exe':'');

                $this->pidfile = $app->runtimePath($this->config->sys->pid);

                if(!$this->start())
                    throw new \Exception('Autostart of Warlock server has failed!');

            }

        }

        $headers = array();

        if($this->config->admin->key !== null)
            $headers['X-WARLOCK-ADMIN-KEY'] = base64_encode($this->config->admin->key);

        if(!$this->connect($this->config->sys['application_name'], $this->config->client['server'], $this->config->client['port'], $headers)) {

            $this->disconnect(FALSE);

            if($autostart)
                throw new \Exception('Warlock was started, but we were unable to communicate with it.');
            elseif($require_connect === true)
                throw new \Exception('Unable to communicate with Warlock.  Is it running?');

        }

    }

    static public function getInstance($autostart = null, $config = null, $require_connect = true){

        $instance_key = hash('crc32b', $config['client']['server'] . $config['client']['port']);

        if(!(array_key_exists($instance_key, Control::$instance) && Control::$instance[$instance_key] instanceof Control))
            Control::$instance[$instance_key] = new Control($autostart, $config, $instance_key, $require_connect);

        return Control::$instance[$instance_key];

    }

    public function isRunning() {

        if(!$this->pidfile)
            throw new \Exception('Can not check for running Warlock instance without PID file!');

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

        if(!$this->pidfile)
            return false;

        if($this->isRunning())
            return true;

        $php_binary = $this->config->sys['php_binary'];

        if(! file_exists($php_binary))
            throw new \Exception('The PHP CLI binary does not exist at ' . $php_binary);

        if(! is_executable($php_binary))
            throw new \Exception('The PHP CLI binary exists but is not executable!');

        $server = dirname(__FILE__) . DIRECTORY_SEPARATOR . 'Server.php';

        if(!file_exists($server))
            throw new \Exception('Warlock server script could not be found!');

        if(substr(PHP_OS, 0, 3) == 'WIN')
            $this->cmd = 'start ' . ($this->config->server['win_bg']?'/B':'') . ' "Hazaar Warlock" "' . $php_binary . '" "' . $server . '"';
        else
            $this->cmd = $php_binary . ' ' . $server;

        $env = $_SERVER;

        $env['APPLICATION_PATH'] = APPLICATION_PATH;

        $env['APPLICATION_ENV'] = APPLICATION_ENV;

        $env['WARLOCK_EXEC'] = 1;

        if(substr(PHP_OS, 0, 3) !== 'WIN')
            $env['WARLOCK_OUTPUT'] = 'file';

        foreach($env as $name => $value)
            putenv($name . '=' . $value);

        //Start the server.  This should work on Linux and Windows
        pclose(popen($this->cmd, "r"));

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

        $this->send('status');

        if($this->recv($packet) == 'STATUS')
            return $packet;

        return false;

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
            return $payload->job_id;

        return FALSE;

    }

    public function schedule($when, \Closure $code, $params = NULL, $tag = NULL, $overwrite = FALSE) {

        $function = new \Hazaar\Closure($code);

        $data = array(
            'application' => array(
                'path' => APPLICATION_PATH,
                'env'  => APPLICATION_ENV
            ),
            'when'        => strtotime($when),
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
            return $payload->job_id;

        return FALSE;

    }

    public function cancel($job_id) {

        $this->send('cancel', $job_id);

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

    public function service($name){

        $this->send('service', $name);

        if($this->recv($payload) == 'SERVICE')
            return $payload;

        return false;

    }

}
