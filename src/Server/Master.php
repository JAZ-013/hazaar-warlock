<?php

namespace Hazaar\Warlock\Server;

define('STREAM_MAX_RECV_LEN', 65535);

/**
 * Warlock Server Master Class
 *
 * The Master class is the root class of the Warlock server.  It is responsible for starting and managing the TCP socket
 * server and connections.  It also creates and starts the Cluster class that is basically the brains of the server.  While
 * the Master class handles all inbound streams, the data it grabs from these streams is passed to the Cluster class for
 * processing.
 *
 */
class Master {

    /**
     * MASTER SERVER INSTANCE
     *
     * @var \Hazaar\Warlock\Server\Master
     */
    static public $instance;

    /**
     * The Warlock protocol encoder/decoder object.
     *
     * This object must be shared by all classes that want to encode/decode packets as it contains the system ID.
     *
     * @var \Hazaar\Warlock\Protocol
     */
    static public $protocol;

    /**
     * The Warlock Cluster Manager
     *
     * @var \Hazaar\Warlock\Server\Cluster
     */
    static public $cluster;

    /**
     * Enable silent mode
     *
     * When enabled, all output will be redirected to a file and nothing will be written to the console.
     *
     * @var boolean
     */
    private $silent = false;

    /**
     * Main loop state.
     *
     * On false, Warlock will exit the main loop and terminate
     *
     * @var boolean
     */
    private $running = NULL;

    /**
     * Shutdown delay timer
     *
     * If this value is a non-NULL epoch value in the past, then the server will shutdown.  Setting this to a value in
     * the future allows a shutdown to be scheduled.
     *
     * @var int
     */
    private $shutdown = NULL;

    /**
     * The Warlock Server configuration object
     *
     * @var \Hazaar\Application\Config
     */
    public $config;

    /**
     * A Logger object
     *
     * @var \Hazaar\Warlock\Server\Logger
     */
    public $log;

    /**
     * Job tags
     *
     * @var mixed
     */
    private $tags = array();

    /**
     * Epoch of when Warlock was started
     *
     * @var mixed
     */
    public $start = 0;

    /**
     * Epoch of the last time stuff was processed
     * @var mixed
     */
    public $time = 0;

    /**
     * Current process id
     *
     * @var mixed
     */
    public $pid = 0;

    /**
     * Current process id file
     * @var mixed
     */
    private $pidfile;

    /**
     * Default select() timeout
     *
     * @var mixed
     */
    private $tv = 1;

    /**
     * SOCKETS & STREAMS
     */

    /**
     * The main socket for listening for incomming connections.
     *
     * @var resource
     */
    private $master = NULL;

    /**
     * Currently connected stream resources we are listening for data on.
     *
     * @var array
     */
    private $streams = array();

    /**
     * Current connection objects that are linked to streams
     *
     * @var array
     */
    private $connections = array();

    // Signals that we will capture
    public $pcntl_signals = array(
        SIGINT  => 'SIGINT',
        SIGHUP  => 'SIGHUP',
        SIGTERM => 'SIGTERM',
        SIGQUIT => 'SIGQUIT'
    );

    private $exit_codes = array(
        1 => array(
            'lvl' => W_ERR,
            'msg' => 'Service failed to start because the application failed to decode the start payload.'
        ),
        2 => array(
            'lvl' => W_ERR,
            'msg' => 'Service failed to start because the application runner does not understand the start payload type.'
        ),
        3 => array(
            'lvl' => W_ERR,
            'msg' => 'Service failed to start because service class does not exist.'
        ),
        4 => array(
            'lvl' => W_WARN,
            'msg' => 'Service exited because it lost the control channel.',
            'restart' => true,
        ),
        5 => array(
            'lvl' => W_WARN,
            'msg' => 'Dynamic service failed to start because it has no runOnce() method!'
        ),
        6 => array(
            'lvl' => W_INFO,
            'msg' => 'Service exited because it\'s source file was modified.',
            'restart' => true,
            'reset' => true
        )
    );

    /**
     * Warlock server constructor
     *
     * The constructor here is responsible for setting up internal structures, initialising logging, RRD
     * logging, redirecting output to log files and configuring error and exception handling.
     *
     * @param boolean $silent By default, log output will be displayed on the screen.  Silent mode will redirect all
     * log output to a file.
     */
    function __construct($silent = false) {

        \Hazaar\Warlock\Config::$default_config['sys']['id'] = crc32(APPLICATION_PATH);

        \Hazaar\Warlock\Config::$default_config['sys']['pid'] = 'warlock-' . APPLICATION_ENV . '.pid';

        \Hazaar\Warlock\Config::$default_config['cluster']['name'] = crc32(APPLICATION_PATH . APPLICATION_ENV);

        global $STDOUT;

        global $STDERR;

        Master::$instance = $this;

        $this->silent = $silent;

        if(!$this->loadConfig())
            throw new \Exception('There is no warlock configuration file.  Warlock is disabled!');

        Logger::set_default_log_level($this->config->log->level);

        $this->log = new Logger();

        set_error_handler(array($this, '__errorHandler'));

        set_exception_handler(array($this, '__exceptionHandler'));

        if($tz = $this->config->sys['timezone'])
            date_default_timezone_set($tz);

        self::$cluster = new Cluster($this->config['cluster']);

        if ($this->config->admin->key === '0000') {

            $msg = '* USING DEFAULT ADMIN KEY!!!  PLEASE CONSIDER SETTING server.key IN warlock config!!! *';

            $this->log->write(W_WARN, str_repeat('*', strlen($msg)));

            $this->log->write(W_WARN, $msg);

            $this->log->write(W_WARN, str_repeat('*', strlen($msg)));

        }

        if ($this->silent) {

            if ($this->config->log->file) {

                fclose(STDOUT);

                $STDOUT = fopen($this->runtimePath($this->config->log->file), 'a');

            }

            if ($this->config->log->error) {

                fclose(STDERR);

                $STDERR = fopen($this->runtimePath($this->config->log->error), 'a');

            }

        }

        $this->log->write(W_INFO, 'Warlock starting up...');

        $this->pid = getmypid();

        $this->pidfile = $this->runtimePath($this->config->sys->pid);

        $this->log->write(W_NOTICE, 'PHP Version = ' . PHP_VERSION);

        $this->log->write(W_NOTICE, 'Application path = ' . APPLICATION_PATH);

        $this->log->write(W_NOTICE, 'Application name = ' . APPLICATION_NAME);

        $this->log->write(W_NOTICE, 'Library path = ' . LIBRAY_PATH);

        $this->log->write(W_NOTICE, 'Application environment = ' . APPLICATION_ENV);

        $this->log->write(W_NOTICE, 'PID = ' . $this->pid);

        $this->log->write(W_NOTICE, 'PID file = ' . $this->pidfile);

        $this->log->write(W_NOTICE, 'Server ID = ' . $this->config->sys->id);

        $this->log->write(W_NOTICE, 'Listen address = ' . $this->config->server->listen);

        $this->log->write(W_NOTICE, 'Listen port = ' . $this->config->server->port);

        Master::$protocol = new \Hazaar\Warlock\Protocol($this->config->sys->id, $this->config->server->encoded, true);

    }

    final public function __errorHandler($errno , $errstr , $errfile = null, $errline  = null, $errcontext = array()){

        if($errno === 2)
            return;

        $type_map = array(
            E_ERROR         => W_ERR,
            E_WARNING       => W_WARN,
            E_NOTICE        => W_NOTICE,
            E_CORE_ERROR    => W_ERR,
            E_CORE_WARNING  => W_WARN,
            E_USER_ERROR    => W_ERR,
            E_USER_WARNING  => W_WARN,
            E_USER_NOTICE   => W_NOTICE
        );

        $type = ake($type_map, $errno, W_ERR);

        $this->log->write($type, "ERROR #$errno on line $errline of $errfile - $errstr");

    }

    final public function __exceptionHandler($e){

        $this->log->write(W_ERR, "MASTER EXCEPTION #{$e->getCode()} - {$e->getMessage()}");

        $this->log->write(W_DEBUG, "EXCEPTION File: {$e->getFile()}");

        $this->log->write(W_DEBUG, "EXCEPTION Line: {$e->getLine()}");

        if($this->log->getLevel() >= W_DEBUG){

            echo str_repeat('-', 40) . "\n";

            debug_print_backtrace();

            echo str_repeat('-', 40) . "\n";

        }

    }

    static private function __signalHandler($signo, $siginfo) {

        if(!($master = Master::$instance) instanceof Master)
            return false;

        $master->log->write(W_DEBUG, 'Got signal: ' . $master->pcntl_signals[$signo]);

        switch ($signo) {
            case SIGHUP :

                if($master->loadConfig() === false)
                    $master->log->write(W_ERR, "Reloading configuration failed!  Config disappeared?");

                break;

            case SIGINT:
            case SIGTERM:
            case SIGQUIT:



                $master->shutdown();

                break;

        }

        return true;

    }

    /**
     * Load the server configuration file.
     *
     * @return \Hazaar\Application\Config
     */
    public function loadConfig(){

        if($this->log)
            $this->log->write(W_NOTICE, (($this->config instanceof \Hazaar\Application\Config) ? 'Re-l' : 'L' ) . "oading configuration");

        $config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, \Hazaar\Warlock\Config::$default_config);

        if(!$config->loaded())
            return false;

        return $this->config = $config;

    }

    /**
     * Initiate a server shutdown.
     *
     * Because this server manages running services, it's not really a good idea to just simply exist abruptly. This
     * method will initiate a server shutdown which will nicely stop all services and once all services stop, the
     * server will terminate safely.
     *
     * @param mixed $delay How long in seconds before the shutdown should commence.
     *
     * @return boolean Returns true unless a shutdown has already been requested
     */
    public function shutdown($delay = null) {

        if($this->shutdown > 0)
            return false;

        if($delay === null)
            $delay = 0;

        $this->log->write(W_DEBUG, "SHUTDOWN: DELAY=$delay");

        $this->shutdown = time() + $delay;

        return true;

    }

    /**
     * Final cleanup of the PID file and logs the exit.
     */
    function __destruct() {

        if (file_exists($this->pidfile))
            unlink($this->pidfile);

        $this->log->write(W_INFO, 'Exiting...');

    }

    /**
     * Returns the application runtime directory
     *
     * The runtime directory is a place where HazaarMVC will keep files that it needs to create during
     * normal operation. For example, cached views, and backend applications.
     *
     * @param mixed $suffix An optional suffix to tack on the end of the path
     * @param mixed $create_dir If the runtime directory does not yet exist, try and create it (requires write permission).
     *
     * @since 1.0.0
     *
     * @throws \Exception
     *
     * @return string The path to the runtime directory
     */
    public function runtimePath($suffix = NULL, $create_dir = false) {

        $path = APPLICATION_PATH . DIRECTORY_SEPARATOR . ($this->config->app->has('runtimepath') ? $this->config->app->runtimepath : '.runtime');

        if(!file_exists($path)) {

            $parent = dirname($path);

            if(!is_writable($parent))
                throw new \Exception('Not writable! Can not create runtime path: ' . $path);

            // Try and create the directory automatically
            try {

                mkdir($path, 0775);

            }
            catch(\Exception $e) {

                throw new \Exception('Error creating runtime path: ' . $path);

            }

        }

        if(!is_writable($path))
            throw new \Exception('Runtime path not writable: ' . $path);

        $path = realpath($path);

        if($suffix = trim($suffix)) {

            if($suffix && substr($suffix, 0, 1) != DIRECTORY_SEPARATOR)
                $suffix = DIRECTORY_SEPARATOR . $suffix;

            $full_path = $path . $suffix;

            if(!file_exists($full_path) && $create_dir)
                mkdir($full_path, 0775, true);

        } else {

            $full_path = $path;

        }

        return $full_path;

    }

    /**
     * Check if the server is already running.
     *
     * This checks if the PID file exists, grabs the PID from that file and checks to see if a process with that ID
     * is actually running.
     *
     * @return boolean True if the server is running. False otherwise.
     */
    private function isRunning() {

        if (file_exists($this->pidfile)) {

            $pid = (int) file_get_contents($this->pidfile);

            $proc_file = '/proc/' . $pid . '/stat';

            if(file_exists($proc_file)){

                $proc = file_get_contents($proc_file);

                return ($proc !== '' && preg_match('/^' . preg_quote($pid) . '\s+\(php\)/', $proc));

            }

        }

        return false;

    }

    /**
     * Prepares the server ready to get up and running.
     *
     * Bootstrapping the server allows us to restart an existing server instance without having to reinstantiate
     * it which allows the server to essentially restart itself in memory.
     *
     * @return Master Returns the server instance
     */
    public function bootstrap() {

        if ($this->isRunning())
            throw new \Exception("Warlock is already running.");

        foreach($this->pcntl_signals as $sig => $name)
            pcntl_signal($sig, array($this, '__signalHandler'), true);

        if($this->config->kvstore['enabled'] === true)
            self::$cluster->startKV();

        $this->log->write(W_NOTICE, 'Creating TCP socket stream on: '
            . $this->config->server->listen . ':' . $this->config->server->port);

        if(!($this->master = stream_socket_server('tcp://' . $this->config->server->listen . ':' . $this->config->server->port, $errno, $errstr)))
            throw new \Exception($errstr, $errno);

        $this->log->write(W_NOTICE, 'Configuring TCP socket');

        if (!stream_set_blocking($this->master, 0))
            throw new \Exception('Failed: stream_set_blocking(0)');

        $this->streams[0] = $this->master;

        $this->running = true;

        self::$cluster->start();

        $this->log->write(W_INFO, "Ready...");

        return $this;

    }

    /**
     * The main server run loop
     *
     * This method will not return for as long as the server is running.  While it is running it will
     * process jobs, monitor services and distribute server signals.
     *
     * @return integer Returns an exit code indicating why the server is exiting. 0 means nice shutdown.
     */
    public function run() {

        $this->start = time();

        file_put_contents($this->pidfile, $this->pid);

        while($this->running) {

            pcntl_signal_dispatch();

            if ($this->shutdown !== NULL && $this->shutdown <= time())
                $this->running = false;

            if (!$this->running)
                break;

            $read = $this->streams;

            $write = $except = NULL;

            if (@stream_select($read, $write, $except, $this->tv) > 0) {

                $this->tv = 0;

                foreach($read as $stream) {

                    if ($stream === $this->master) {

                        $client_stream = stream_socket_accept($stream);

                        if ($client_stream < 0) {

                            $this->log->write(W_ERR, "Failed: socket_accept()");

                            continue;

                        } else {

                            $conn = new Connection($client_stream);

                            $stream_id = $this->addConnection($conn);

                            $this->log->write(W_NOTICE, "Connection from $conn->address:$conn->port with stream id #$stream_id");

                            //Unset the conn variable.  This fixes a quirk where the last connection object is never destroyed
                            unset($conn);

                        }

                    } else {

                        if($this->processStream($stream) !== true)
                            $this->disconnect($stream);

                    }

                }

            } else {

                $this->tv = 1;

            }

            $now = time();

            if($this->time < $now){

                self::$cluster->process();

                $this->time = $now;

            }

        }

        self::$cluster->stop();

        $this->log->write(W_NOTICE, 'Closing all remaining connections');

        foreach($this->streams as $stream)
            fclose($stream);

        return 0;

    }

    /**
     * Add a new connection to stream input handler
     *
     * @param Connection $conn
     * @return boolean|integer
     */
    public function addConnection(Connection $conn){

        if(!($stream = $conn->getReadStream()))
            return false;

        $stream_id = intval($stream);

        $this->streams[$stream_id] = $stream;

        $this->connections[$stream_id] = $conn;

        $this->log->write(W_DEBUG, 'MASTER->ADDCONNECTION: STREAM=' . $stream_id);

        return $stream_id;

    }

    /**
     * Remove an existing connection from the stream input handler
     *
     * This should be done when a connection closes.
     *
     * @param Connection $conn
     * @return boolean
     */
    public function removeConnection(Connection $conn){

        if(!($stream = $conn->getReadStream()))
            return false;

        $stream_id = intval($stream);

        if(array_key_exists($stream_id, $this->streams)){

            $this->log->write(W_DEBUG, 'MASTER->REMOVESTREAM: STREAM=' . $stream_id);

            unset($this->streams[$stream_id]);

        }

        if(array_key_exists($stream_id, $this->connections)){

            $this->log->write(W_DEBUG, 'MASTER->REMOVECONNECTION: STREAM=' . $stream_id);

            unset($this->connections[$stream_id]);

        }

        return true;

    }

    /**
     * Retrieve a connection object for a stream resource
     *
     * @param resource $stream The stream resource
     *
     * @return Connection
     */
    public function getConnection($stream) {

        $stream_id = intval($stream);

        if(array_key_exists($stream_id, $this->connections))
            return $this->connections[$stream_id];

        return null;

    }

    /**
     * Process data input on a stream
     *
     * @param resource $stream The stream to read data from
     *
     * @return boolean Result of data processing.  True is good.  False is bad.
     */
    public function processStream($stream) {

        $buf = fread($stream, STREAM_MAX_RECV_LEN);

        $conn = $this->getConnection($stream);

        if(!$conn instanceof Connection)
            return false;

        return $conn->recv($buf);

    }

    /**
     * Disconnect a stream resource
     *
     * If the resource is linked with an existing connection, then the connection object is used to handle
     * the disconnect.  That way any closing frames are sent, subscriptions are cleared out, etc.
     *
     * Otherwise, we shutdown the connection immediately and close the stream resource.
     *
     * @param mixed $stream
     * @return boolean
     */
    public function disconnect($stream) {

        if ($conn = $this->getConnection($stream))
            return $conn->disconnect(true);

        $stream_id = intval($stream);

        /**
         * Remove the stream from our list of streams
         */
        if (array_key_exists($stream_id, $this->streams))
            unset($this->streams[$stream_id]);

        $this->log->write(W_DEBUG, "STREAM_CLOSE: STREAM=" . $stream);

        stream_socket_shutdown($stream, STREAM_SHUT_RDWR);

        return fclose($stream);

    }

}
