<?php

namespace Hazaar\Warlock\Server;

class Master {

    /**
     * SERVER INSTANCE
     */
    static public $instance;

    // The Warlock protocol encoder/decoder.
    static public $protocol;

    static public $cluster;

    private $silent = false;

    // Main loop state. On false, Warlock will exit the main loop and terminate
    private $running = NULL;

    private $shutdown = NULL;

    public $config;

    public $log;

    /**
     * Job tags
     * @var mixed
     */
    private $tags = array();

    /**
     * Epoch of when Warlock was started
     *
     * @var mixed
     */
    private $start = 0;

    /**
     * Epoch of the last time stuff was processed
     * @var mixed
     */
    private $time = 0;

    /**
     * Current process id
     *
     * @var mixed
     */
    private $pid = 0;

    /**
     * Current process id file
     * @var mixed
     */
    private $pidfile;

    /**
     * Default select() timeout
     * @var mixed
     */
    private $tv = 1;

    private $stats = array(
        'clients' => 0,         // Total number of connected clients
        'processed' => 0,       // Total number of processed jobs & events
        'execs' => 0,           // The number of successful job executions
        'lateExecs' => 0,       // The number of delayed executions
        'failed' => 0,          // The number of failed job executions
        'processes' => 0,       // The number of currently running processes
        'retries' => 0,         // The total number of job retries
        'queue' => 0,           // Current number of jobs in the queue
        'limitHits' => 0,       // The number of hits on the process limiter
        'events' => 0,          // The number of events triggered
        'subscriptions' => 0    // The number of waiting client connections
    );

    /**
     * JOBS & SERVICES
     */

    // Currently running processes (jobs AND services)
    private $processes = array();

    // Application services
    private $services = array();

    /**
     * QUEUES
     */

    // Main job queue
    public $jobQueue = array();

    /**
     * SOCKETS & STREAMS
     */

    // The main socket for listening for incomming connections.
    private $master = NULL;

    // Currently connected stream resources we are listening for data on.
    public $streams = array();

    private $connections = array();

    private $admins = array();

    private $kv_store;

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

        if(($this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, \Hazaar\Warlock\Config::$default_config)) === false)
            throw new \Exception('There is no warlock configuration file.  Warlock is disabled!');

        Logger::set_default_log_level($this->config->log->level);

        $this->log = new Logger();

        set_error_handler(array($this, '__errorHandler'));

        set_exception_handler(array($this, '__exceptionHandler'));

        self::$cluster = new Cluster($this->config);

        if(!$this->config->sys['php_binary'])
            $this->config->sys['php_binary'] = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) === 'WIN')?'.exe':'');

        if($tz = $this->config->sys->get('timezone'))
            date_default_timezone_set($tz);

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

        $this->log->write(W_NOTICE, 'PHP Binary = ' . $this->config->sys['php_binary']);

        $this->log->write(W_NOTICE, 'Application path = ' . APPLICATION_PATH);

        $this->log->write(W_NOTICE, 'Application name = ' . APPLICATION_NAME);

        $this->log->write(W_NOTICE, 'Library path = ' . LIBRAY_PATH);

        $this->log->write(W_NOTICE, 'Application environment = ' . APPLICATION_ENV);

        $this->log->write(W_NOTICE, 'PID = ' . $this->pid);

        $this->log->write(W_NOTICE, 'PID file = ' . $this->pidfile);

        $this->log->write(W_NOTICE, 'Server ID = ' . $this->config->sys->id);

        $this->log->write(W_NOTICE, 'Listen address = ' . $this->config->server->listen);

        $this->log->write(W_NOTICE, 'Listen port = ' . $this->config->server->port);

        $this->log->write(W_NOTICE, 'Job expiry = ' . $this->config->job->expire . ' seconds');

        $this->log->write(W_NOTICE, 'Exec timeout = ' . $this->config->exec->timeout . ' seconds');

        $this->log->write(W_NOTICE, 'Process limit = ' . $this->config->exec->limit . ' processes');

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

    public function loadConfig(){

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

        if(count($this->processes) > 0) {

            $this->log->write(W_WARN, 'Killing with processes with extreme prejudice!');

            foreach($this->processes as $process)
                $process->terminate();

        }

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

        if($this->config->kvstore['enabled'] === true){

            $this->log->write(W_NOTICE, 'Initialising KV Store');

            $this->kv_store = new Kvstore($this->config->kvstore['persist'], $this->config->kvstore['compact']);

        }

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

        $services = new \Hazaar\Application\Config('service', APPLICATION_ENV);

        if ($services->loaded()) {

            $this->log->write(W_INFO, "Checking for enabled services");

            foreach($services as $name => $options) {

                $this->log->write(W_NOTICE, "Found service: $name");

                $options['name'] = $name;

                $this->services[$name] = new Service($options->toArray());

                if ($options['enabled'] === true)
                    $this->serviceEnable($name);

            }

        }

        $this->log->write(W_INFO, "Ready...");

        return $this;

    }

    private function callable($value){

        if($value instanceof \Hazaar\Map)
            $value = $value->toArray();

        if(is_array($value))
            return $value;

        if(strpos($value, '::') === false)
            return null;

        return explode('::', $value, 2);

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

                $this->processJobs();

                if($this->kv_store)
                    $this->kv_store->expireKeys();

                $this->time = $now;

            }

        }

        if (count($this->jobQueue) > 0) {

            $this->log->write(W_NOTICE, 'Terminating running jobs');

            foreach($this->jobQueue as $job){

                if($job->status() === 'running')
                    $job->cancel();

            }

            if(($wait = $this->config->exec['exitWait']) > 0){

                $this->log->write(W_NOTICE, "Waiting for processes to exit (max $wait seconds)");

                $start = time();

                while(count($this->processes) > 0) {

                    if(($start  + $wait) < time()){

                        $this->log->write(W_WARN, 'Timeout reached while waiting for process to exit.');

                        break;

                    }

                    $this->processJobs();

                    if (count($this->processes) === 0)
                        break;

                    sleep(1);

                }

            }

        }

        $this->log->write(W_NOTICE, 'Closing all remaining connections');

        foreach($this->streams as $stream)
            fclose($stream);

        $this->log->write(W_NOTICE, 'Cleaning up');

        $this->streams = array();

        $this->jobQueue = array();

        $this->clients = array();

        return 0;

    }

    public function addConnection(Connection $conn){

        $stream_id = intval($conn->stream);

        $this->streams[$stream_id] = $conn->stream;

        $this->connections[$stream_id] = $conn;

        $this->log->write(W_DEBUG, 'MASTER->ADDCONNECTION: STREAM=' . $stream_id);

        return $stream_id;

    }

    public function removeConnection(Connection $conn){

        $stream_id = intval($conn->stream);

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
     * Retrieve a client object for a stream resource
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

    /*
    public function authorise(CommInterface $client, $payload, &$response){

    if(!($payload instanceof \stdClass
    && property_exists($payload, 'access_key')
    && $payload->access_key === $this->config->admin->key
    )) return false;

    $type = strtoupper(ake($payload, 'type', 'admin'));

    $client->type = $type;

    if($type === 'PEER'){

    $response = self::$cluster->addPeer($client);

    $stream_id = intval($client->socket);

    if(!array_key_exists($stream_id, $this->streams))
    $this->streams[$stream_id] = $client;

    }else{

    $this->log->write(W_NOTICE, 'Warlock control authorised to ' . $client->id, $client->name);

    }

    $this->admins[$client->id] = $client;

    return true;


    }*/

    /*
    private function addClient($stream) {

    // If we don't have a stream or id, return false
    if (!($stream && is_resource($stream)))
    return false;

    $stream_id = intval($stream);

    // If the stream already has a client object, return it
    if (array_key_exists($stream_id, $this->clients))
    return $this->clients[$stream_id];

    //Create the new client object
    $client = new Node($stream, $this->config->client);

    // Add it to the client array
    $this->clients[$stream_id] = $client;

    $this->stats['clients']++;

    return $client;

    }*/

    /**
     * Removes a client from a stream.
     *
     * Because a client can have multiple stream connections (in legacy mode) this removes the client reference
     * for that stream. Once there are no more references left the client is completely removed.
     *
     * @param mixed $stream
     *
     * @return boolean
     */
    /*
    public function removeClient($stream) {

    if (!$stream)
    return false;

    $stream_id = intval($stream);

    if (!array_key_exists($stream_id, $this->clients))
    return false;

    $client = $this->clients[$stream_id];

    foreach($this->waitQueue as $event_id => &$queue){

    if(!array_key_exists($client->id, $queue))
    continue;

    $this->log->write(W_DEBUG, "CLIENT->UNSUBSCRIPE: EVENT=$event_id CLIENT=$client->id", $client->name);

    unset($queue[$client->id]);

    }

    $this->log->write(W_DEBUG, "CLIENT->REMOVE: CLIENT=$client->id", $client->name);

    unset($this->clients[$stream_id]);

    unset($this->streams[$stream_id]);

    $this->stats['clients']--;

    return true;

    }*/

    private function getStatus($full = true) {

        $status = array(
            'state' => 'running',
            'pid' => $this->pid,
            'started' => $this->start,
            'uptime' => time() - $this->start,
            'memory' => memory_get_usage(),
            'stats' => $this->stats,
            'connections' => count($this->streams),
            'clients' => count($this->clients)
        );

        if (!$full)
            return $status;

        $status['clients'] = array();

        $status['queue'] = array();

        $status['processes'] = array();

        $status['services'] = array();

        $status['events'] = array();

        $status['stats']['queue'] = 0;

        foreach($this->clients as $client) {

            $status['clients'][] = array(
                'id' => $client->id,
                'username' => $client->username,
                'since' => $client->since,
                'ip' => $client->address,
                'port' => $client->port,
                'type' => $client->type
            );

        }

        $arrays = array(
            'queue' => $this->jobQueue,             // Main job queue
            'processes' => $this->processes,        // Active process queue
            'services' => $this->services,          // Configured services
            'events' => $this->eventQueue           // Event queue
        );

        foreach($arrays as $name => &$array){

            if($array instanceof \Hazaar\Model\Strict)
                $status['stats'][$name] = $array->count();
            elseif(is_array($array))
                $status['stats'][$name] = count($array);

            if ($name === 'events' && array_key_exists($this->config->admin->trigger, $array)) {

                $status[$name] = array_diff_key($array, array_flip(array(
                    $this->config->admin->trigger
                )));

            } else {

                $status[$name] = $array;

            }

        }

        return $status;

    }

    /**
     * Process administative commands for a client
     *
     * @param Node $client
     * @param mixed $command
     * @param mixed $payload
     *
     * @return mixed
     */
    public function processCommand(Node $client, $command, &$payload) {

        if(!$command)
            return false;

        if($this->kv_store !== NULL && substr($command, 0, 2) === 'KV')
            return $this->kv_store->process($client, $command, $payload);

        if (!($client instanceof Node\Peer || array_key_exists($client->id, $this->admins)))
            throw new \Exception('Admin commands only allowed by authorised clients!');

        $this->log->write(W_DEBUG, "ADMIN_COMMAND: $command" . ($client->id ? " CLIENT=$client->id" : NULL));

        switch ($command) {

            case 'EVENT':

                if(!property_exists($payload, 'trigger'))
                    throw new \Exception('Event triggered without trigger ID!');

                if(array_key_exists($payload->id, $this->eventQueue)
                    && array_key_exists($payload->trigger, $this->eventQueue[$payload->id]))
                    $this->log->write(W_WARN, 'Received existing trigger ' . $payload->trigger . ' for event ' . $payload->id);
                else
                    $this->trigger($payload->id, ake($payload, 'data'), $client->id, $payload->trigger);

                break;

            case 'SHUTDOWN':

                $delay = ake($payload, 'delay', 0);

                $this->log->write(W_NOTICE, "Shutdown requested (Delay: $delay)");

                if(!$this->shutdown($delay))
                    throw new \Exception('Unable to initiate shutdown!');

                $client->send('OK', array('command' => $command));

                break;

            case 'DELAY' :

                $payload->when = time() + ake($payload, 'value', 0);

                $this->log->write(W_DEBUG, "JOB->DELAY: INTERVAL={$payload->value}");

            case 'SCHEDULE' :

                if(!property_exists($payload, 'when'))
                    throw new \Exception('Unable schedule code execution without an execution time!');

                if(!($id = $this->scheduleJob(
                    $payload->when,
                    $payload->exec,
                    $payload->application,
                    ake($payload, 'tag'),
                    ake($payload, 'overwrite', false)
                ))) throw new \Exception('Could not schedule delayed function');

                $client->send('OK', array('command' => $command, 'job_id' => $id));

                break;

            case 'CANCEL' :

                if (!$this->cancelJob($payload))
                    throw new \Exception('Error trying to cancel job');

                $this->log->write(W_NOTICE, "Job successfully cancelled");

                $client->send('OK', array('command' => $command, 'job_id' => $payload));

                break;

            case 'ENABLE' :

                $this->log->write(W_NOTICE, "ENABLE: NAME=$payload CLIENT=$client->id");

                if(!$this->serviceEnable($payload))
                    throw new \Exception('Unable to enable service ' . $payload);

                $client->send('OK', array('command' => $command, 'name' => $payload));

                break;

            case 'DISABLE' :

                $this->log->write(W_NOTICE, "DISABLE: NAME=$payload CLIENT=$client->id");

                if(!$this->serviceDisable($payload))
                    throw new \Exception('Unable to disable service ' . $payload);

                $client->send('OK', array('command' => $command, 'name' => $payload));

                break;

            case 'STATUS':

                $this->log->write(W_NOTICE, "STATUS: CLIENT=$client->id");

                $client->send('STATUS', $this->getStatus());

                break;

            case 'SERVICE' :

                $this->log->write(W_NOTICE, "SERVICE: NAME=$payload CLIENT=$client->id");

                if(!array_key_exists($payload, $this->services))
                    throw new \Exception('Service ' . $payload . ' does not exist!');

                $client->send('SERVICE', $this->services[$payload]);

                break;

            case 'SPAWN':

                if(!($name = ake($payload, 'name')))
                    throw new \Exception('Unable to spawn a service without a service name!');

                if(!($id = $this->spawn($client, $name, $payload)))
                    throw new \Exception('Unable to spawn dynamic service: ' . $name);

                $client->send('OK', array('command' => $command, 'name' => $name, 'job_id' => $id));

                break;

            case 'KILL':

                if(!($name = ake($payload, 'name')))
                    throw new \Exception('Can not kill dynamic service without a name!');

                if(!$this->kill($client, $name))
                    throw new \Exception('Unable to kill dynamic service ' . $name);

                $client->send('OK', array('command' => $command, 'name' => $payload));

                break;

            case 'SIGNAL':

                if(!($event_id = ake($payload, 'id')))
                    return false;

                //Otherwise, send this signal to any child services for the requested type
                if(!($service = ake($payload, 'service')))
                    return false;

                if(!$this->signal($client, $event_id, $service, ake($payload, 'data')))
                    throw new \Exception('Unable to signal dynamic service');

                $client->send('OK', array('command' => $command, 'name' => $payload));

                break;

            default:

                throw new \Exception('Unhandled command: ' . $command);

        }

        return true;

    }

    private function spawn(Node $client, $name, $options){

        if (!array_key_exists($name, $this->services))
            return false;

        $service = & $this->services[$name];

        $job = new Job\Service(array(
            'name' => $name,
            'start' => time(),
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'tag' => $name,
            'enabled' => true,
            'dynamic' => true,
            'detach' => ake($options, 'detach', false),
            'respawn' => false,
            'parent' => $client,
            'params' => ake($options, 'params'),
            'loglevel' => $service->loglevel
        ));

        $this->log->write(W_NOTICE, 'Spawning dynamic service: ' . $name, $job->id);

        $client->jobs[$job->id] = $this->queueAddJob($job);

        return $job->id;

    }

    private function kill(Node $client, $name){

        if (!array_key_exists($name, $this->services))
            return false;

        foreach($client->jobs as $id => $job){

            if($job->name !== $name)
                continue;

            $this->log->write(W_NOTICE, "KILL: SERVICE=$name JOB_ID=$id CLIENT={$client->id}");

            $job->cancel();

            unset($client->jobs[$id]);

        }

        return true;

    }

    private function signal(Node $client, $event_id, $service, $data = null){

        $trigger_id = uniqid();

        //If this is a message coming from the service, send it back to it's parent client connection
        if($client->type === 'SERVICE'){

            if(count($client->jobs) === 0)
                throw new \Exception('Client has no associated jobs!');

            foreach($client->jobs as $id => $job){

                if(!(array_key_exists($id, $this->jobQueue) && $job->name === $service && $job->parent instanceof Node))
                    continue;

                $this->log->write(W_NOTICE, "SERVICE->SIGNAL: SERVICE=$service JOB_ID=$id CLIENT={$client->id}");

                $job->parent->sendEvent($event_id, $trigger_id, $data);

            }

        }else{

            if(count($client->jobs) === 0)
                throw new \Exception('Client has no associated jobs!');

            foreach($client->jobs as $id => $job){

                if(!(array_key_exists($id, $this->jobQueue) && $job->name === $service))
                    continue;

                $this->log->write(W_NOTICE, "CLIENT->SIGNAL: SERVICE=$service JOB_ID=$id CLIENT={$client->id}");

                $job->sendEvent($event_id, $trigger_id, $data);

            }

        }

        return true;

    }

    public function scheduleJob($when, $exec, $application, $tag = NULL, $overwrite = false) {

        if(!property_exists($exec, 'callable')){

            $this->log->write(W_ERR, 'Unable to schedule job without function callable!');

            return false;

        }

        if($tag === null && is_array($exec->callable)){

            $tag = md5(implode('-', $exec->callable));

            $overwrite = true;

        }

        if ($tag && array_key_exists($tag, $this->tags)) {

            $job = $this->tags[$tag];

            $this->log->write(W_NOTICE, "Job already scheduled with tag $tag", $job->id);

            if ($overwrite === false){

                $this->log->write(W_NOTICE, 'Skipping', $job->id);

                return false;

            }

            $this->log->write(W_NOTICE, 'Overwriting', $job->id);

            $job->cancel();

            unset($this->tags[$tag]);

            unset($this->jobQueue[$job->id]);

        }

        $job = new Job\Runner(array(
            'when' => $when,
            'application' => array(
                'env' => $application->env
            ),
            'exec' => $exec->callable,
            'params' => ake($exec, 'params', array()),
            'timeout' => $this->config->exec->timeout
        ));

        $when = $job->touch();

        $this->log->write(W_DEBUG, "JOB: ID=$job->id");

        $this->log->write(W_DEBUG, 'WHEN: ' . date($this->config->sys['date_format'], $job->start), $job->id);

        $this->log->write(W_DEBUG, 'APPLICATION_ENV:  ' . $application->env, $job->id);

        if (!$when || $when < time()) {

            $this->log->write(W_WARN, 'Trying to schedule job to execute in the past', $job->id);

            return false;

        }

        if($tag){

            $this->log->write(W_DEBUG, 'TAG: ' . $tag, $job->id);

            $this->tags[$tag] = $job;

        }

        $this->log->write(W_NOTICE, 'Scheduling job for execution at ' . date($this->config->sys['date_format'], $when), $job->id);

        $this->queueAddJob($job);

        $this->stats['queue']++;

        return $job->id;

    }

    private function cancelJob($job_id) {

        $this->log->write(W_DEBUG, 'Trying to cancel job', $job_id);

        //If the job IS is not found return false
        if (!array_key_exists($job_id, $this->jobQueue))
            return false;

        $job =& $this->jobQueue[$job_id];

        if ($job->tag)
            unset($this->tags[$job->tag]);

        /**
         * Stop the job if it is currently running
         */
        if ($job->status === STATUS_RUNNING) {

            if ($job->process) {

                $this->log->write(W_NOTICE, 'Stopping running ' . $job->type);

                $job->process->termiante();

            } else {

                $this->log->write(W_ERR, ucfirst($job->type) . ' has running status but proccess resource was not found!');

            }

        }

        $job->status = STATUS_CANCELLED;

        // Expire the job in 30 seconds
        $job->expire = time() + $this->config->job->expire;

        return true;

    }

    /*
     * Main job processor loop This is the main loop that executed scheduled jobs.
     * It uses proc_open to execute jobs in their own process so
     * that they don't interfere with other scheduled jobs.
     */
    private function processJobs() {

        foreach($this->jobQueue as $id => &$job){

            //Jobs that are queued and ready to execute or ready to restart an execution retry.
            if ($job->ready()){

                $now = time();

                if (count($this->processes) >= $this->config->exec->limit) {

                    $this->stats['limitHits']++;

                    $this->log->write(W_WARN, 'Process limit of ' . $this->config->exec->limit . ' processes reached!');

                    break;

                }

                if ($job instanceof Job\Runner){

                    if ($job->retries > 0)
                        $this->stats['retries']++;

                    if($job->event === false){

                        $this->log->write(W_NOTICE, "Starting job execution", $id);

                        $this->log->write(W_DEBUG, 'NOW:  ' . date($this->config->sys['date_format'], $now), $id);

                        $this->log->write(W_DEBUG, 'WHEN: ' . date($this->config->sys['date_format'], $job->start), $id);

                        if ($job->retries > 0)
                            $this->log->write(W_DEBUG, 'RETRIES: ' . $job->retries, $id);

                    }

                    $late = $now - $job->start;

                    $job->late = $late;

                    if ($late > 0) {

                        $this->log->write(W_DEBUG, "LATE: $late seconds", $id);

                        $this->stats['lateExecs']++;

                    }

                }

                /*
                 * Open a new process to php CLI and pipe the function out to it for execution
                 */
                $process = new Process(array(
                    'id' => $id,
                    'type' => $job->type,
                    'tag' => $job->tag,
                    'application' => $job->application
                ), $this->config);

                if($process->is_running()) {

                    $this->processes[$id] = $process;

                    $job->process = $process;

                    $job->status = STATUS_RUNNING;

                    $this->stats['processes']++;

                    if(!($root = getenv('APPLICATION_ROOT'))) $root = '/';

                    $payload = array(
                        'application_name' => APPLICATION_NAME,
                        'timezone' => date_default_timezone_get(),
                        'config' => array('app' => array('root' => $root))
                    );

                    $packet = null;

                    if ($job instanceof Job\Service) {

                        $payload['name'] = $job->name;

                        if($config = $job->config)
                            $payload['config'] = array_merge($payload['config'], $config);

                        $packet = Master::$protocol->encode('service', $payload);

                    } elseif ($job instanceof Job\Runner) {

                        $payload['exec'] = $job->exec;

                        if ($job->has('params') && is_array($job->params) && count($job->params) > 0)
                            $payload['params'] = $job->params;

                        $packet = Master::$protocol->encode('exec', $payload);

                    }

                    $pipe = $process->getReadPipe();

                    $pipe_id = intval($pipe);

                    $this->streams[$pipe_id] = $pipe;

                    $this->clients[$pipe_id] = $job;

                    $process->start($packet);

                } else {

                    $job->status = STATUS_ERROR;

                    // Expire the job when the queue expiry is reached
                    $job->expire = time() + $this->config->job->expire;

                    $this->log->start(W_ERR, 'Could not create child process.  Execution failed', $id);

                }

            } elseif ($job->expired()) { //Clean up any expired jobs (completed or errored)

                if($job->status === STATUS_CANCELLED){

                    $this->log->write(W_NOTICE, 'Killing cancelled process', $id);

                    $job->process->terminate();

                }

                $this->log->write(W_NOTICE, 'Cleaning up', $id);

                $this->stats['queue']--;

                if ($job instanceof Job\Service)
                    unset($this->services[$job->name]['job']);

                unset($this->jobQueue[$id]);

            }elseif($job->status === STATUS_RUNNING || $job->status === STATUS_CANCELLED){

                if(!is_object($job->process)){

                    $this->log->write(W_ERR, 'Service has running status, but no process linked!');

                    $job->status = STATUS_ERROR;

                    continue;

                }

                $status = $job->process->status;

                if ($status['running'] === false) {

                    $this->log->write(W_DEBUG, "PROCESS->STOP: PID=$status[pid] ID=" . $job->process->id);

                    $pipe = $job->process->getReadPipe();

                    //Do any last second processing.  Usually shutdown log messages.
                    if($buffer = stream_get_contents($pipe))
                        $job->recv($buffer);

                    //One last check of the error buffer
                    if(($output = $job->process->readErrorPipe()) !== false)
                        $this->log->write(W_ERR, "PROCESS ERROR:\n$output");

                    //Now remove everything and clean up
                    unset($this->processes[$job->process->id]);

                    $this->stats['processes']--;

                    $this->removeClient($pipe);

                    $job->disconnect();

                    /**
                     * Process a Service shutdown.
                     */
                    if ($job instanceof Job\Service) {

                        $name = $job->name;

                        $this->services[$name]['status'] = 'stopped';

                        $this->log->write(W_DEBUG, "SERVICE=$name EXIT=$status[exitcode]");

                        if ($status['exitcode'] !== 0 && $job->status !== STATUS_CANCELLED) {

                            $this->log->write(W_NOTICE, "Service returned status code $status[exitcode]", $name);

                            if(!($ec = ake($this->exit_codes, $status['exitcode'])))
                                $ec = array(
                                    'lvl' => W_WARN,
                                    'msg' => 'Service exited unexpectedly.',
                                    'restart' => true
                                );

                            $this->log->write($ec['lvl'], $ec['msg'], $name);

                            if(ake($ec, 'restart', false) !== true){

                                $this->log->write(W_ERR, 'Disabling the service.', $name);

                                $job->status = STATUS_ERROR;

                                continue;

                            }

                            if(ake($ec, 'reset', false) === true)
                                $job->retries = 0;

                            if ($job->retries > $this->config->service->restarts) {

                                if($job->dynamic === true){

                                    $this->log->write(W_WARN, "Dynamic service is restarting too often.  Cancelling spawn.", $name);

                                    $this->cancelJob($job->id);

                                }else{

                                    $this->log->write(W_WARN, "Service is restarting too often.  Disabling for {$this->config->service->disable} seconds.", $name);

                                    $job->start = time() + $this->config->service->disable;

                                    $job->retries = 0;

                                    $job->expire = 0;

                                }

                            } else {

                                $this->log->write(W_NOTICE, "Restarting service"
                                    . (($job->retries > 0) ? " ({$job->retries})" : null), $name);

                                if (array_key_exists($job->service, $this->services))
                                    $this->services[$job->service]['restarts']++;

                                $job->retries++;

                                $job->status = STATUS_QUEUED_RETRY;

                            }

                        } elseif ($job->respawn === true && $job->status === STATUS_RUNNING) {

                            $this->log->write(W_NOTICE, "Respawning service in "
                                . $job->respawn_delay . " seconds.", $name);

                            $job->start = time() + $job->respawn_delay;

                            $job->status = STATUS_QUEUED;

                            if (array_key_exists($job->service, $this->services))
                                $this->services[$job->service]['restarts']++;

                        } else {

                            $job->status = STATUS_COMPLETE;

                            $job->expire = time();

                        }

                    } else {

                        $this->log->write(W_NOTICE, "Process exited with return code: " . $status['exitcode'], $id);

                        if ($status['exitcode'] > 0) {

                            $this->log->write(W_WARN, 'Execution completed with error.', $id);

                            if($job->event === true) {

                                $job->status = STATUS_ERROR;

                            }elseif ($job->retries >= $this->config->job->retries) {

                                $this->log->write(W_ERR, 'Cancelling job due to too many retries.', $id);

                                $job->status = STATUS_ERROR;

                                $this->stats['failed']++;

                            }else{

                                $this->log->write(W_NOTICE, 'Re-queuing job for execution.', $id);

                                $job->status = STATUS_QUEUED_RETRY;

                                $job->start = time() + $this->config->job->retry;

                                $job->retries++;

                            }

                        } else {

                            $this->log->write(W_NOTICE, 'Execution completed successfully.', $id);

                            $this->stats['execs']++;

                            if(($next = $job->touch()) > time()){

                                $job->status = STATUS_QUEUED;

                                $job->retries = 0;

                                $this->log->write(W_NOTICE, 'Next execution at: ' . date($this->config->sys['date_format'], $next), $id);

                            }else{

                                $job->status = STATUS_COMPLETE;

                                // Expire the job in 30 seconds
                                $job->expire = time() + $this->config->job->expire;

                            }

                        }

                    }

                } elseif ($status['running'] === TRUE) {

                    try{

                        //Receive any error from STDERR
                        if(($output = $job->process->readErrorPipe()) !== false)
                            $this->log->write(W_ERR, "PROCESS ERROR:\n$output");

                    }
                    catch(\Throwable $e){

                        $this->log->write(W_ERR, 'EXCEPTION #'
                            . $e->getCode()
                            . ' on line ' . $e->getLine()
                            . ' in file ' . $e->getFile()
                            . ': ' . $e->getMessage());

                    }

                }

            } elseif ($job instanceof Job\Runner
                        && $job->status === STATUS_RUNNING
                        && $job->timeout()) {

                $this->log->write(W_WARN, "Process taking too long to execute - Attempting to kill it.", $id);

                if ($job->process->terminate())
                    $this->log->write(W_DEBUG, 'Terminate signal sent.', $id);
                else
                    $this->log->write(W_ERR, 'Failed to send terminate signal.', $id);

            }

        }

    }

    public function queueAddJob(Job $job){

        if(array_key_exists($job->id, $this->jobQueue)){

            $this->log->write(W_WARN, 'Job already exists in queue!', $job->id);

        }else{

            $job->status = STATUS_QUEUED;

            $this->jobQueue[$job->id] = $job;

            $this->stats['queue']++;

            $this->log->write(W_DEBUG, "JOB->QUEUE: START=" . date($this->config->sys['date_format'], $job->start) . " TAG=$job->tag", $job->id);

        }

        $this->processJobs();

        return $job;

    }

    private function serviceEnable($name) {

        if (!array_key_exists($name, $this->services))
            return false;

        $service = $this->services[$name];

        $this->log->write(W_INFO, 'Enabling service: ' . $name . (($service->delay > 0) ? ' (delay=' . $service->delay . ')': null));

        $service->enabled = true;

        $job = new Job\Service(array(
            'name' => $service->name,
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'tag' => $name,
            'respawn' => false,
            'loglevel' => $service->loglevel
        ));

        if($service->delay > 0)
            $job->start = time() + $service->delay;

        $this->services[$name]->job = $this->queueAddJob($job);

        return true;

    }

    private function serviceDisable($name) {

        if (!array_key_exists($name, $this->services))
            return false;

        $service = &$this->services[$name];

        if(!$service->enabled)
            return false;

        $this->log->write(W_INFO, 'Disabling service: ' . $name);

        return $service->disable($this->config->job->expire);

    }

}
