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

    static private $guid;

    static private $instance = array();

    function __construct($autostart = NULL, $config = null, $instance_key = null, $require_connect = true) {

        Config::$default_config['sys']['id'] = crc32(APPLICATION_PATH);

        Config::$default_config['sys']['application_name'] = APPLICATION_NAME;

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, Config::$default_config);

        if(!$this->config->loaded())
            throw new \Exception('There is no warlock configuration file.  Warlock is disabled!');

        if($config)
            $this->config->extend($config);

        if(!$instance_key)
            $instance_key = hash('crc32b', $this->config->client['server'] . $this->config->client['port']);

        if(array_key_exists($instance_key, Control::$instance))
            throw new \Exception('There is already a control instance for this server:host.  Please use ' . __CLASS__ . '::getInstance()');

        Control::$instance[$instance_key] = $this;

        $application = \Hazaar\Application::getInstance();

        $protocol = new Protocol($this->config->sys->id, $this->config->server->encoded);

        if(!Control::$guid){

            $guid_file = $application->runtimePath('warlock.guid');

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
                    $this->config->sys['php_binary'] = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ($this->isWindowsOS()?'.exe':'');

                $this->pidfile = $application->runtimePath($this->config->sys->pid);

                if(!$this->start())
                    throw new \Exception('Autostart of Warlock server has failed!');

            }

        }

        parent::__construct($application, $protocol, Control::$guid);

        if(!$this->connected()){

            if($autostart)
                throw new \Exception('Warlock was started, but we were unable to communicate with it.');
            elseif($require_connect === true)
                throw new \Exception('Unable to communicate with Warlock.  Is it running?');

        }

    }

    protected function connect(\Hazaar\Warlock\Protocol $protocol, $guid = null){

        $headers = array();

        if($this->config->admin->key !== null)
            $headers['X-WARLOCK-ACCESS-KEY'] = base64_encode($this->config->admin->key);

        if($this->config->client['port'] === null)
            $this->config->client['port'] = $this->config->server['port'];

        if($this->config->client['server'] === null){

            if(trim($this->config->server['listen']) == '0.0.0.0')
                $this->config->client['server'] = '127.0.0.1';
            else
                $this->config->client['server'] = $this->config->server['listen'];

        }

        $conn = new Connection\Socket($protocol, Control::$guid);

        if(!$conn->connect($this->config->sys['application_name'], $this->config->client['server'], $this->config->client['port'], $headers))
            $conn->disconnect(FALSE);

        if($conn->recv($payload) !== 'OK')
            return false;

        return $conn;

    }

    static public function getInstance($autostart = null, $config = null, $require_connect = true){

        $instance_key = hash('crc32b', $config['client']['server'] . $config['client']['port']);

        if(!(array_key_exists($instance_key, Control::$instance) && Control::$instance[$instance_key] instanceof Control))
            Control::$instance[$instance_key] = new Control($autostart, $config, $instance_key, $require_connect);

        return Control::$instance[$instance_key];

    }

    private function makeCallable($callable){

        if(!is_callable($callable))
            throw new \Exception('Function must be callable!');

        if($callable instanceof \Closure){

            $callable = (string)new \Hazaar\Closure($callable);

        }elseif(is_array($callable) && is_object($callable[0])){

            $reflectionMethod = new \ReflectionMethod($callable[0], $callable[1]);

            $classname = get_class($callable[0]);

            if(!$reflectionMethod->isStatic())
                throw new \Exception('Method ' . $callable[1] . ' of class ' . $classname . ' must be static');

            $callable[0] = $classname;

        }elseif(is_string($callable) && strpos($callable, '::')){

            $callable = explode('::', $callable);

        }

        return array('callable' => $callable);

    }

    private function isWindowsOS($except_on_wsl_missing = false){

        if(substr(PHP_OS, 0, 3) !== 'WIN')
            return false;

        if($except_on_wsl_missing === true
            && !file_exists(dirname(getenv('ComSpec')) . DIRECTORY_SEPARATOR . 'wsl.exe'))
            throw new \Exception('Hazaar Warlock requires Windows Subsystem for Linux (WSL) in order to run on Windows.');

        return true;

    }

    public function isRunning() {

        if(!$this->pidfile)
            throw new \Exception('Can not check for running Warlock instance without PID file!');

        if(!file_exists($this->pidfile))
            return false;

        if(!($pid = (int)file_get_contents($this->pidfile)))
            return false;

        if($this->isWindowsOS(true)){

            $descriptorspec = array(
                0 => array("pipe", "r"),
                1 => array("pipe", "w"),
                2 => array("pipe", "w")
            );

            //We have to use proc_open because WSL dies without a STDIN pipe.
            $process = proc_open('wsl FILE=/proc/' . $pid . '/stat; if [ -e $FILE ] ; then cat $FILE; fi;', $descriptorspec, $pipes);

            if(!is_resource($process))
                throw new \Exception('Unable to inspect processes in WSL.');

            do{

                $status = proc_get_status($process);

            }while($status['running'] === true);

            if($error = stream_get_contents($pipes[2]))
                throw new \Exception($error);

            $proc = stream_get_contents($pipes[1]);

            proc_close($process);

        }else{

            $proc_file = '/proc/' . $pid . '/stat';

            if(!file_exists($proc_file))
                return false;

            $proc = file_get_contents($proc_file);

        }

        return ($proc !== '' && preg_match('/^' . preg_quote($pid) . '\s+\(php\)/', $proc));

    }

    public function start($timeout = NULL) {

        if(!$this->pidfile)
            return false;

        if($this->isRunning())
            return true;

        $env = array(
            'APPLICATION_PATH'  => APPLICATION_PATH,
            'APPLICATION_ENV'   => APPLICATION_ENV,
            'APPLICATION_ROOT'  => \Hazaar\Application::getRoot(),
            'WARLOCK_EXEC'      => 1
        );

        $php_options = array();

        if(function_exists('xdebug_is_debugger_active') && xdebug_is_debugger_active()){

            $env['XDEBUG_CONFIG'] = 'profiler_enable=1'
                . ' remote_enable='             . ini_get('xdebug.remote_enable')
                . ' remote_handler='            . ini_get('xdebug.remote_handler')
                . ' remote_mode='               . ini_get('xdebug.remote_mode')
                . ' remote_port='               . ini_get('xdebug.remote_port')
                . ' remote_host='               . ini_get('xdebug.remote_host')
                . ' remote_cookie_expire_time=' . ini_get('xdebug.remote_cookie_expire_time');

        }

        if($this->isWindowsOS(true)){

            if(PHP_INT_SIZE !== 8)
                throw new \Exception('Autostart of warlock is only supported on 64-bit environments.');

            $php_options[] = str_replace(DIRECTORY_SEPARATOR, '/', '..'
                . str_replace(realpath(getcwd() . DIRECTORY_SEPARATOR . '..'), '', dirname(__FILE__) . '/Server.php'));

            $this->cmd = 'start /I /MAX /WAIT ' . (($this->config->server['win_bg'] === true)?'/B ':'') . '"Hazaar Warlock" "cmd" "/K wsl php'
                . ' ' . implode(' ', $php_options);

            $env['WSLENV'] = 'APPLICATION_PATH/p:APPLICATION_ENV:APPLICATION_ROOT:WARLOCK_EXEC:WARLOCK_OUTPUT:XDEBUG_CONFIG';

        }else{

            $php_options[] = $server = dirname(__FILE__) . DIRECTORY_SEPARATOR . 'Server.php';

            if(!file_exists($server))
                throw new \Exception('Warlock server script could not be found!');

            $this->cmd = $this->config->sys['php_binary'] . ' ' .  implode(' ', $php_options);

            $env['WARLOCK_OUTPUT'] = 'file';

        }

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

    public function runDelay($delay, $callable, $params = null, $tag = null, $overwrite = false) {

        return $this->sendExec('delay', array('value' => $delay), $callable, $params, $tag, $overwrite);

    }

    public function interval($value, $callable, $params = null, $tag = null, $overwrite = false) {

        return $this->sendExec('interval', array('value' => $value), $callable, $params, $tag, $overwrite);

    }

    public function schedule($when, $callable, $params = null, $tag = null, $overwrite = false) {

        return $this->sendExec('schedule', array('when' => $when), $callable, $params, $tag, $overwrite);

    }

    private function sendExec($command, $data, $callable, $params = null, $tag = null, $overwrite = false){

        $data['application'] = array(
            'env'  => APPLICATION_ENV
        );

        $data['exec'] = $this->makeCallable($callable);

        if($tag) {

            $data['tag'] = $tag;

            $data['overwrite'] = strbool($overwrite);

        }

        if($params !== null && !is_array($params))
            $params = array($params);

        $data['exec']['params'] = $params;

        $this->send($command, $data);

        if($this->recv($payload) == 'OK')
            return $payload->job_id;

        return false;

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
