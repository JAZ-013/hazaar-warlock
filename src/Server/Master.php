<?php

namespace Hazaar\Warlock\Server;

class Master {

    /**
     * SERVER INSTANCE
     */
    static public $instance;

    private $silent = false;

    // Main loop state. On false, Warlock will exit the main loop and terminate
    private $running = NULL;

    private $shutdown = NULL;

    public $config;

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

    private $rrd;

    private $rrdfile;

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

    // The wait queue. Clients subscribe to events and are added to this array.
    private $waitQueue = array();

    // The Event queue. Holds active events waiting to be seen.
    private $eventQueue = array();

    /**
     * SOCKETS & STREAMS
     */

    // The main socket for listening for incomming connections.
    private $master = NULL;

    // Currently connected sockets we are listening for data on.
    private $sockets = array();

    // Currently connected clients.
    private $clients = array();

    private $admins = array();

    // The Warlock protocol encoder/decoder.
    static public $protocol;

    private $kv_store;

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

        global $STDOUT;

        global $STDERR;

        Master::$instance = $this;

        $this->silent = $silent;

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, \Hazaar\Warlock\Config::$default_config);

        if(!$this->config->loaded())
            throw new \Exception('There is no warlock configuration file.  Warlock is disabled!');

        Logger::set_default_log_level($this->config->log->level);

        $this->log = new Logger();

        set_error_handler(array($this, '__errorHandler'));

        set_exception_handler(array($this, '__exceptionHandler'));

        if(!$this->config->sys['php_binary'])
            $this->config->sys['php_binary'] = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) == 'WIN')?'.exe':'');

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

        if ($this->rrdfile = $this->runtimePath($this->config->log->rrd)) {

            $this->rrd = new \Hazaar\File\RRD($this->rrdfile, 60);

            if (!$this->rrd->exists()) {

                $this->rrd->addDataSource('sockets', 'GAUGE', 60, NULL, NULL, 'Socket Connections');

                $this->rrd->addDataSource('clients', 'GAUGE', 60, NULL, NULL, 'Clients');

                $this->rrd->addDataSource('memory', 'GAUGE', 60, NULL, NULL, 'Memory Usage');

                $this->rrd->addDataSource('jobs', 'COUNTER', 60, NULL, NULL, 'Job Executions');

                $this->rrd->addDataSource('events', 'COUNTER', 60, NULL, NULL, 'Events');

                $this->rrd->addDataSource('services', 'GAUGE', 60, NULL, NULL, 'Enabled Services');

                $this->rrd->addDataSource('processes', 'GAUGE', 60, NULL, NULL, 'Running Processes');

                $this->rrd->addArchive('permin_1hour', 'MAX', 0.5, 1, 60, 'Max per minute for the last hour');

                $this->rrd->addArchive('perhour_100days', 'AVERAGE', 0.5, 60, 2400, 'Average per hour for the last 100 days');

                $this->rrd->addArchive('perday_1year', 'AVERAGE', 0.5, 1440, 365, 'Average per day for the last year');

                $this->rrd->create();

            }

        }

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

        Master::$protocol = new \Hazaar\Application\Protocol($this->config->sys->id, $this->config->server->encoded);

    }

    final public function __errorHandler($errno , $errstr , $errfile = null, $errline  = null, $errcontext = array()){

        echo str_repeat('-', 40) . "\n";

        echo "MASTER ERROR #$errno\nFile: $errfile\nLine: $errline\n\n$errstr\n";

        echo str_repeat('-', 40) . "\n";


    }

    final public function __exceptionHandler($e){

        $this->log(W_ERR, "MASTER EXCEPTION #{$e->getCode()} - {$e->getMessage()}");

        $this->log(W_DEBUG, "EXCEPTION File: {$e->getFile()}");

        $this->log(W_DEBUG, "EXCEPTION Line: {$e->getLine()}");

        if($this->log->getLevel() >= W_DEBUG){

            echo str_repeat('-', 40) . "\n";

            debug_print_backtrace();

            echo str_repeat('-', 40) . "\n";

        }

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
     * normal operation. For example, socket files for background scheduler communication, cached views,
     * and backend applications.
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

            if (file_exists('/proc/' . $pid)) {

                $this->pid = $pid;

                return true;

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

        if($this->config->server['kvstore'] === true){

            $this->log->write(W_NOTICE, 'Initialising KV Store');

            $this->kv_store = new Kvstore($this->log);

        }

        $this->log->write(W_NOTICE, 'Creating TCP socket');

        $this->master = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if (!$this->master)
            throw new \Exception('Unable to create AF_UNIX socket');

        $this->log->write(W_NOTICE, 'Configuring TCP socket');

        if (!socket_set_option($this->master, SOL_SOCKET, SO_REUSEADDR, 1))
            throw new \Exception('Failed: socket_option()');

        $this->log->write(W_NOTICE, 'Binding to socket on '
            . $this->config->server->listen . ':' . $this->config->server->port);

        if (!socket_bind($this->master, $this->config->server->listen, $this->config->server->port))
            throw new \Exception('Unable to bind to ' . $this->config->server->listen . ' on port ' . $this->config->server->port);

        if (!socket_listen($this->master))
            throw new \Exception('Unable to listen on ' . $this->config->server->listen . ':' . $this->config->server->port);

        $this->sockets[0] = $this->master;

        $this->running = true;

        $this->log->write(W_INFO, "Ready...");

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

            if ($this->shutdown !== NULL && $this->shutdown <= time())
                $this->running = false;

            if (!$this->running)
                break;

            $read = $this->sockets;

            $write = $except = NULL;

            if (@socket_select($read, $write, $except, $this->tv) > 0) {

                $this->tv = 0;

                foreach($read as $socket) {

                    if ($socket == $this->master) {

                        $client_socket = socket_accept($socket);

                        if ($client_socket < 0) {

                            $this->log->write(W_ERR, "Failed: socket_accept()");

                            continue;

                        } else {

                            $socket_id = intval($client_socket);

                            $this->sockets[$socket_id] = $client_socket;

                            socket_getpeername($client_socket, $address, $port);

                            $this->log->write(W_NOTICE, "Connection from " . $address . ':' . $port);

                        }

                    } else {

                        $this->processClient($socket);

                    }

                }

            } else {

                $this->tv = 1;

            }

            $now = time();

            if($this->time < $now){

                $this->processJobs();

                $this->queueCleanup();

                $this->checkClients();

                if($this->kv_store)
                    $this->kv_store->expireKeys();

                $this->rrd->setValue('sockets', count($this->sockets));

                $this->rrd->setValue('clients', count($this->clients));

                $this->rrd->setValue('memory', memory_get_usage());

                if ($this->rrd->update())
                    gc_collect_cycles();

                $this->time = $now;

            }

        }

        if (count($this->processes) > 0) {

            $this->log->write(W_NOTICE, 'Terminating running processes');

            foreach($this->processes as $process)
                $process->send('cancel');

            $this->log->write(W_NOTICE, 'Waiting for processes to exit');

            $start = time();

            while(count($this->processes) > 0) {

                if ($start >= time() + 10) {

                    $this->log->write(W_WARN, 'Timeout reached while waiting for process to exit.');

                    break;

                }

                $this->processJobs();

                if (count($this->procs) == 0)
                    break;

                sleep(1);

            }

        }

        $this->log->write(W_NOTICE, 'Closing all connections');

        foreach($this->sockets as $socket)
            socket_close($socket);

        $this->sockets = array();

        $this->log->write(W_NOTICE, 'Cleaning up');

        $this->jobQueue = array();

        $this->clients = array();

        $this->eventQueue = array();

        $this->waitQueue = array();

        return 0;

    }

    public function authorise(Client $client, $key, $job_id = null){

        if($job_id !== null){

            if(!array_key_exists($job_id, $this->jobQueue))
                return false;

            $job = $this->jobQueue[$job_id];

            if($job->access_key !== $key)
                return false;

            if($job->registerClient($client))
                $this->log->write(W_NOTICE, ucfirst($client->type) . ' registered successfully', $job_id);

        }else{

            if($key !== $this->config->admin->key)
                return false;

            $this->log->write(W_NOTICE, 'Warlock control authorised to ' . $client->id);

            $client->type = 'admin';

        }

        $this->admins[$client->id] = $client;

        return true;

    }

    private function addClient($socket) {

        // If we don't have a socket or id, return false
        if (!($socket && is_resource($socket)))
            return false;

        $socket_id = intval($socket);

        // If the socket already has a client object, return it
        if (array_key_exists($socket_id, $this->clients))
            return $this->clients[$socket_id];

        //Create the new client object
        $client = new Client($socket, $this->config->client);

        // Add it to the client array
        $this->clients[$socket_id] = $client;

        $this->stats['clients']++;

        return $client;

    }

    /**
     * Retrieve a client object for a socket resource
     *
     * @param mixed $socket The socket resource
     *
     * @return Client|null
     */
    private function getClient($socket) {

        $socket_id = intval($socket);

        return (array_key_exists($socket_id, $this->clients) ? $this->clients[$socket_id] : NULL);

    }

    /**
     * Removes a client from a socket.
     *
     * Because a client can have multiple socket connections (in legacy mode) this removes the client reference
     * for that socket. Once there are no more references left the client is completely removed.
     *
     * @param mixed $socket
     *
     * @return boolean
     */
    public function removeClient($socket) {

        if (!$socket)
            return false;

        $socket_id = intval($socket);

        if (!array_key_exists($socket_id, $this->clients))
            return false;

        $client = $this->clients[$socket_id];

        foreach($this->waitQueue as &$queue){

            if(array_key_exists($client->id, $queue))
                unset($queue[$client->id]);

        }

        $this->log->write(W_DEBUG, "REMOVE: CLIENT=$socket_id");

        unset($this->clients[$socket_id]);

        unset($this->sockets[$socket_id]);

        $this->stats['clients']--;

        return true;

    }

    private function processClient($socket) {

        @$bytes_received = socket_recv($socket, $buf, 65536, 0);

        if ($bytes_received == 0) {

            $this->log->write(W_NOTICE, 'Remote host closed connection');

            $this->disconnect($socket);

            return false;

        }

        @$status = socket_getpeername($socket, $address, $port);

        if ($status == false) {

            $this->disconnect($socket);

            return false;

        }

        $this->log->write(W_DEBUG, "SOCKET_RECV: HOST=$address:$port BYTES=" . strlen($buf));

        $client = $this->getClient($socket);

        if ($client instanceof Client) {

            $client->recv($buf);

        }else{

            if (!($client = $this->addClient($socket)))
                $this->disconnect($socket);

            if(!$client->initiateHandshake($buf)){

                $this->removeClient($socket);

                socket_close($socket);

            }

        }

        return true;

    }

    public function disconnect($socket) {

        $socket_id = intval($socket);

        if ($client = $this->getClient($socket))
            return $client->disconnect();

        /**
         * Remove the socket from our list of sockets
         */
        if (array_key_exists($socket_id, $this->sockets))
            unset($this->sockets[$socket_id]);

        $this->log->write(W_DEBUG, "SOCKET_CLOSE: SOCKET=" . $socket);

        socket_close($socket);

    }

    private function checkClients(){

        if(!(is_array($this->clients) && count($this->clients) > 0))
            return;

        //Only ping if we havn't received data from the client for the configured number of seconds (default to 60).
        $when = time() - $this->config->client->check;

        foreach($this->clients as $client){

            if($client->lastContact <= $when)
                $client->ping();

        }

        return;

    }

    private function getStatus($full = true) {

        $status = array(
            'state' => 'running',
            'pid' => $this->pid,
            'started' => $this->start,
            'uptime' => time() - $this->start,
            'memory' => memory_get_usage(),
            'stats' => $this->stats,
            'connections' => count($this->sockets),
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

            if ($name == 'events' && array_key_exists($this->config->admin->trigger, $array)) {

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
     * @param Client $client
     * @param mixed $command
     * @param mixed $payload
     *
     * @return mixed
     */
    public function processCommand(Client $client, $command, &$payload) {

        if($this->kv_store !== NULL && substr($command, 0, 2) === 'KV')
            return $this->kv_store->process($client, $command, $payload);

        if (!($command && array_key_exists($client->id, $this->admins)))
            throw new \Exception('Admin commands only allowed by authorised clients!');

        $this->log->write(W_DEBUG, "ADMIN_COMMAND: $command" . ($client->id ? " CLIENT=$client->id" : NULL));

        switch ($command) {

            case 'SHUTDOWN':

                $delay = ake($payload, 'delay', 0);

                $this->log->write(W_NOTICE, "Shutdown requested (Delay: $delay)");

                if(!$this->shutdown($delay))
                    throw new \Exception('Unable to initiate shutdown!');

                $client->send('OK', array('command' => $command));

                break;

            case 'DELAY' :

                $payload->when = time() + ake($payload, 'value', 0);

                $this->log->write(W_NOTICE, "Scheduling delayed job for {$payload->value} seconds");

            case 'SCHEDULE' :

                if(!property_exists($payload, 'when'))
                    throw new \Exception('Unable schedule code execution without an execution time!');

                if(!($id = $this->scheduleJob(
                    $payload->when,
                    $payload->exec,
                    $payload->application,
                    ake($command, 'tag'),
                    ake($command, 'overwrite', false)
                ))) throw new \Exception('Could not schedule delayed function');

                $this->log->write(W_NOTICE, 'Job successfully scheduled', $id);

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

    public function trigger($event_id, $data, $client_id = null) {

        $this->log->write(W_NOTICE, "TRIGGER: $event_id");

        $this->stats['events']++;

        $this->rrd->setValue('events', 1);

        $trigger_id = uniqid();

        $seen = array();

        if($client_id > 0)
            $seen[] = $client_id;

        $this->eventQueue[$event_id][$trigger_id] = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'when' => time(),
            'data' => $data,
            'seen' => $seen
        );

        // Check to see if there are any clients waiting for this event and send notifications to them all.
        $this->processSubscriptionQueue($event_id, $trigger_id);

        return true;

    }

    private function spawn($client, $name, $options){

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

    private function kill($client, $name){

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

    private function signal(Client $client, $event_id, $service, $data = null){

        $trigger_id = uniqid();

        //If this is a message coming from the service, send it back to it's parent client connection
        if($client->type === 'service'){

            if(count($client->jobs) === 0)
                throw new \Exception('Client has no associated jobs!');

            foreach($client->jobs as $id => $job){

                if(!(array_key_exists($id, $this->jobQueue) && $job->name === $service && $job->parent instanceof Client))
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

    /**
     * Subscribe a client to an event
     *
     * @param mixed $client The client to subscribe
     * @param mixed $event_id The event ID to subscribe to
     * @param mixed $filter Any event filters
     */
    public function subscribe(Client $client, $event_id, $filter) {

        $this->waitQueue[$event_id][$client->id] = array(
            'client' => $client,
            'since' => time(),
            'filter' => $filter
        );

        if ($event_id == $this->config->admin->trigger)
            $this->log->write(W_DEBUG, "ADMIN_SUBSCRIBE: CLIENT=$client->id");
        else
            $this->stats['subscriptions']++;

        /*
         * Check to see if this subscribe request has any active and unseen events waiting for it.
         */
        $this->processEventQueue($client, $event_id, $filter);

    }

    /**
     * Unsubscibe a client from an event
     *
     * @param mixed $client The client to unsubscribe
     * @param mixed $event_id The event ID to unsubscribe from
     *
     * @return boolean
     */
    public function unsubscribe(Client $client, $event_id) {

        if (!(array_key_exists($event_id, $this->waitQueue)
            && is_array($this->waitQueue[$event_id])
            && array_key_exists($client->id, $this->waitQueue[$event_id])))
            return false;

        $this->log->write(W_DEBUG, "DEQUEUE: NAME=$event_id CLIENT=$client->id");

        unset($this->waitQueue[$event_id][$client->id]);

        if ($event_id == $this->config->admin->trigger)
            $this->log->write(W_DEBUG, "ADMIN_UNSUBSCRIBE: CLIENT=$client->id");
        else
            $this->stats['subscriptions']--;

        return true;

    }

    private function scheduleJob($when, $exec, $application, $tag = NULL, $overwrite = false) {

        if(!property_exists($exec, 'callable')){

            $this->log->write(W_ERR, 'Unable to schedule job without function callable!');

            return false;

        }

        $job = new Job\Runner(array(
            'start' => $when,
            'application' => array(
                'path' => $application->path,
                'env' => $application->env
            ),
            'exec' => $exec->callable,
            'params' => ake($exec, 'params', array()),
            'timeout' => $this->config->exec->timeout
        ));

        $this->log->write(W_DEBUG, "JOB: ID=$job->id");

        if (!is_numeric($when)) {

            $this->log->write(W_DEBUG, 'Parsing string time', $job->id);

            $when = strtotime($when);

        }

        $this->log->write(W_NOTICE, 'NOW:  ' . date('c'), $job->id);

        $this->log->write(W_NOTICE, 'WHEN: ' . date('c', $when), $job->id);

        $this->log->write(W_NOTICE, 'APPLICATION_PATH: ' . $application->path, $job->id);

        $this->log->write(W_NOTICE, 'APPLICATION_ENV:  ' . $application->env, $job->id);

        if (!$when || $when < time()) {

            $this->log->write(W_WARN, 'Trying to schedule job to execute in the past', $job->id);

            return false;

        }

        $this->log->write(W_NOTICE, 'Scheduling job for execution at ' . date('c', $when), $job->id);

        if ($tag) {

            $this->log->write(W_NOTICE, 'TAG: ' . $tag, $job->id);

            if (array_key_exists($tag, $this->tags)) {

                $this->log->write(W_NOTICE, "Job already scheduled with tag $tag", $job->id);

                if ($overwrite === false){

                    $this->log->write(W_NOTICE, 'Skipping', $job->id);

                    return false;

                }

                $this->log->write(W_NOTICE, 'Overwriting', $job->id);

                $this->tags[$tag]->cancel();

            }

            $this->tags[$tag] = $job;

        }

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
        if ($job->status == STATUS_RUNNING) {

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

                    $this->rrd->setValue('jobs', 1);

                    $this->log->write(W_NOTICE, "Starting job execution", $id);

                    $this->log->write(W_DEBUG, 'NOW:  ' . date('c', $now), $id);

                    $this->log->write(W_DEBUG, 'WHEN: ' . date('c', $job->start), $id);

                    if ($job->retries > 0)
                        $this->log->write(W_DEBUG, 'RETRIES: ' . $job->retries, $id);

                    $late = $now - $job->start;

                    $job->late = $late;

                    if ($late > 0) {

                        $this->log->write(W_DEBUG, "LATE: $late seconds", $id);

                        $this->stats['lateExecs']++;

                    }

                    if (is_array($job->params) && count($job->params) > 0){

                        $pout = 'PARAMS: ';

                        foreach($job->params as $param)
                            $pout .= var_export($param, true);

                        $this->log->write(W_NOTICE, $pout, $id);

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
                        'server_port' => $this->config->server['port'] ,
                        'job_id' => $id,
                        'access_key' => $job->access_key,
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

                    $process->start($packet);

                } else {

                    $job->status = STATUS_ERROR;

                    // Expire the job when the queue expiry is reached
                    $job->expire = time() + $this->config->job->expire;

                    $this->log->start(W_ERR, 'Could not create child process.  Execution failed', $id);

                }

            } elseif ($job->expired()) { //Clean up any expired jobs (completed or errored)

                $this->log->write(W_NOTICE, 'Cleaning up', $id);

                $this->stats['queue']--;

                if ($job instanceof Job\Service)
                    unset($this->services[$job->name]['job']);

                unset($this->jobQueue[$id]);

            }elseif($job->status === STATUS_RUNNING){

                if(!is_object($job->process)){

                    $this->log->write(W_ERR, 'Service has running status, but no process linked!');

                    $job->status = STATUS_ERROR;

                    continue;

                }

                $status = $job->process->status;

                if ($status['running'] === false) {

                    unset($this->processes[$job->process->id]);

                    $job->process->close();

                    $job->process = null;

                    $this->stats['processes']--;

                    /**
                     * Process a Service shutdown.
                     */
                    if ($job instanceof Job\Service) {

                        $name = $job->name;

                        $this->services[$name]['status'] = 'stopped';

                        $this->log->write(W_DEBUG, "SERVICE=$name EXIT=$status[exitcode]");

                        if ($status['exitcode'] > 0 && $job->status !== STATUS_CANCELLED) {

                            $this->log->write(W_ERR, "Service returned status code $status[exitcode]", $name);

                            if ($status['exitcode'] == 4) {

                                $this->log->write(W_WARN, 'Service exited because it lost the control channel. Restarting.');

                            } elseif ($status['exitcode'] == 6) {

                                $job->retries = 0;

                                $this->log->write(W_INFO, 'Service exited because it\'s source file was modified.', $name);

                            }else{

                                if ($status['exitcode'] == 1) {

                                    $this->log->write(W_ERR, 'Service failed to start because the application failed to decode the start payload.', $name);

                                } elseif ($status['exitcode'] == 2) {

                                    $this->log->write(W_ERR, 'Service failed to start because the application runner does not understand the start payload type.', $name);

                                } elseif ($status['exitcode'] == 3) {

                                    $this->log->write(W_ERR, 'Service failed to start because service class does not exist.', $name);

                                } elseif ($status['exitcode'] == 5) {

                                    $this->log->write(W_ERR, 'Dynamic service failed to start because it has no runOnce() method!', $name);

                                }

                                $this->log->write(W_ERR, 'Disabling the service.', $name);

                                $job->status = STATUS_ERROR;

                                continue;

                            }

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

                        } elseif ($job->respawn == true && $job->status == STATUS_RUNNING) {

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

                            if ($job->retries >= $this->config->job->retries) {

                                $this->log->write(W_ERR, 'Cancelling job due to too many retries.', $id);

                                $job->status = STATUS_ERROR;

                                $this->stats['failed']++;

                            } else {

                                $this->log->write(W_NOTICE, 'Re-queuing job for execution.', $id);

                                $job->status = STATUS_QUEUED_RETRY;

                                $job->start = time() + $this->config->job->retry;

                                $job->retries++;

                            }

                        } else {

                            $this->log->write(W_NOTICE, 'Execution completed successfully.', $id);

                            $this->stats['execs']++;

                            $job->status = STATUS_COMPLETE;

                            // Expire the job in 30 seconds
                            $job->expire = time() + $this->config->job->expire;

                        }

                    }

                } elseif ($job->status === STATUS_CANCELLED) {

                    $this->log->write(W_NOTICE, 'Killing cancelled process', $id);

                    $job->process->terminate();

                } elseif ($job instanceof Job\Runner && $job->timeout()) {

                    $this->log->write(W_WARN, "Process taking too long to execute - Attempting to kill it.", $id);

                    if ($job->process->terminate()) {

                        $this->log->write(W_DEBUG, 'Terminate signal sent.', $id);

                    } else {

                        $this->log->write(W_ERR, 'Failed to send terminate signal.', $id);

                    }

                }

            }

        }

    }

    private function queueAddJob(Job $job){

        if(array_key_exists($job->id, $this->jobQueue)){

            $this->log->write(W_WARN, 'Job already exists in queue!', $job->id);

        }else{

            $job->status = STATUS_QUEUED;

            $this->jobQueue[$job->id] = $job;

            $this->stats['queue']++;

            $this->log->write(W_NOTICE, 'Job added to queue', $job->id);

        }

        return $job;

    }

    private function queueCleanup() {

        if (!is_array($this->eventQueue))
            $this->eventQueue = array();

        if($this->config->sys['cleanup'] === false)
            return;

        if (count($this->eventQueue) > 0) {

            foreach($this->eventQueue as $event_id => $events) {

                foreach($events as $id => $data) {

                    if (($data['when'] + $this->config->event->queue_timeout) <= time()) {

                        if ($event_id != $this->config->admin->trigger) {

                            $this->log->write(W_DEBUG, "EXPIRE: NAME=$event_id TRIGGER=$id");

                        }

                        unset($this->eventQueue[$event_id][$id]);

                    }

                }

                if (count($this->eventQueue[$event_id]) == 0)
                    unset($this->eventQueue[$event_id]);

            }

        }

    }

    private function fieldExists($search, $array) {

        reset($search);

        while($field = current($search)) {

            if (!property_exists($array, $field))
                return false;

            $array = &$array->$field;

            next($search);

        }

        return true;

    }

    private function getFieldValue($search, $array) {

        reset($search);

        while($field = current($search)) {

            if (!property_exists($array, $field))
                return false;

            $array = &$array->$field;

            next($search);

        }

        return $array;

    }

    /**
     * Tests whether a event should be filtered.
     *
     * Returns true if the event should be filtered (skipped), and false if the event should be processed.
     *
     * @param string $event
     *            The event to check.
     *
     * @param Array $filter
     *            The filter rule to test against.
     *
     * @return bool Returns true if the event should be filtered (skipped), and false if the event should be processed.
     */
    private function filterEvent($event, $filter = NULL) {

        if (!$filter instanceof \stdClass)
            return false;

        $this->log->write(W_DEBUG, 'Checking event filter for \'' . $event['id'] . '\'');

        foreach($filter as $field => $data) {

            $field = explode('.', $field);

            if (!$this->fieldExists($field, $event['data']))
                return true;

            $field_value = $this->getFieldValue($field, $event['data']);

            if ($data instanceof \stdClass) { // If $data is an array it's a complex filter

                foreach($data as $filter_type => $filter_value) {

                    switch ($filter_type) {
                        case 'is' :

                            if ($field_value != $filter_value)
                                return true;

                            break;

                        case 'not' :

                            if ($field_value == $filter_value)
                                return true;

                            break;

                        case 'like' :

                            if (!preg_match($filter_value, $field_value))
                                return true;

                            break;

                        case 'in' :

                            if (!in_array($field_value, $filter_value))
                                return true;

                            break;

                        case 'nin' :

                            if (in_array($field_value, $filter_value))
                                return true;

                            break;

                    }

                }

            } else { // Otherwise it's a simple filter with an acceptable value in it

                if ($field_value != $data)
                    return true;

            }

        }

        return false;

    }

    /**
     * @detail This method is executed when a client connects to see if there are any events waiting in the event
     * queue that the client has not yet seen.
     * If there are, the first event found is sent to the client,
     * marked as seen and then processing stops.
     *
     * @param Client $client
     *
     * @param string $event_id
     *
     * @param Array $filter
     *
     * @return boolean
     */
    private function processEventQueue($client, $event_id, $filter = NULL) {

        $this->log->write(W_NOTICE, "PROCESSING EVENT QUEUE: $event_id");

        if (array_key_exists($event_id, $this->eventQueue)) {

            foreach($this->eventQueue[$event_id] as $trigger_id => &$event) {

                if (!array_key_exists('seen', $event) || !is_array($event['seen']))
                    $event['seen'] = array();

                if (!in_array($client->id, $event['seen'])) {

                    if (!array_key_exists($event_id, $client->subscriptions))
                        continue;

                    if ($this->filterEvent($event, $filter))
                        continue;

                    $event['seen'][] = $client->id;

                    if ($event_id != $this->config->admin->trigger)
                        $this->log->write(W_NOTICE, "SEEN: NAME=$event_id TRIGGER=$trigger_id CLIENT=" . $client->id);

                    if (!$client->sendEvent($event['id'], $trigger_id, $event['data']))
                        return false;

                }

            }

        }

        return true;

    }

    /**
     * @detail This method is executed when a event is triggered.
     * It is responsible for sending events to clients
     * that are waiting for the event and marking them as seen by the client.
     *
     * @param string $event_id
     *
     * @param string $trigger_id
     *
     * @return boolean
     */
    private function processSubscriptionQueue($event_id, $trigger_id = NULL) {

        if (!array_key_exists($event_id, $this->eventQueue))
            return false;

        $this->log->write(W_DEBUG, "EVENT_QUEUE: NAME=$event_id COUNT=" . count($this->eventQueue[$event_id]));

        // Get a list of triggers to process
        $triggers = (empty($trigger_id) ? array_keys($this->eventQueue[$event_id]) : array($trigger_id));

        foreach($triggers as $trigger) {

            if (!isset($this->eventQueue[$event_id][$trigger]))
                continue;

            $event = &$this->eventQueue[$event_id][$trigger];

            if (!array_key_exists($event_id, $this->waitQueue))
                continue;

            foreach($this->waitQueue[$event_id] as $client_id => $item) {

                if (in_array($client_id, $event['seen'])
                    || $this->filterEvent($event, $item['filter']))
                    continue;

                $event['seen'][] = $client_id;

                if ($event_id != $this->config->admin->trigger)
                    $this->log->write(W_DEBUG, "SEEN: NAME=$event_id TRIGGER=$trigger CLIENT={$client_id}");

                if (!$item['client']->sendEvent($event_id, $trigger, $event['data']))
                    return false;

            }

        }

        return true;

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
