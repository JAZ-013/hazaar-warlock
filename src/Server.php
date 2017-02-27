<?php

namespace Hazaar\Warlock;

if (!extension_loaded('sockets'))
    die('The sockets extension is not loaded.');

/**
 * STATUS CONSTANTS
 */
define('STATUS_QUEUED', 0);

define('STATUS_QUEUED_RETRY', 1);

define('STATUS_STARTING', 2);

define('STATUS_RUNNING', 3);

define('STATUS_COMPLETE', 4);

define('STATUS_CANCELLED', 5);

define('STATUS_ERROR', 6);

/**
 * LOGGING CONSTANTS
 */
define('W_INFO', 0);

define('W_ERR', 1);

define('W_WARN', 2);

define('W_NOTICE', 3);

define('W_DEBUG', 4);

define('W_DECODE', 5);

if (!defined('APPLICATION_PATH'))
    define('APPLICATION_PATH', realpath(getenv('APPLICATION_PATH')));

if (!APPLICATION_PATH)
    die("Warlock can not start without an application path.  Make sure APPLICATION_PATH environment variable is set.\n");

if (!(is_dir(APPLICATION_PATH)
    && file_exists(APPLICATION_PATH . '/configs')
    && file_exists(APPLICATION_PATH . '/controllers')))
    die("Application path '" . APPLICATION_PATH . "' is not a valid application directory!\n");

define('APPLICATION_ENV', (getenv('APPLICATION_ENV') ? getenv('APPLICATION_ENV') : 'development'));

define('LIBRAY_PATH', realpath(dirname(__FILE__) . '/../src'));

set_include_path(implode(PATH_SEPARATOR, array(
    realpath(LIBRAY_PATH . '/..'),
    get_include_path()
)));

include APPLICATION_PATH . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR . 'autoload.php';

if(!class_exists('Hazaar\Application'))
    throw new \Exception('A Hazaar application could not be loaded!');

$log_level = W_INFO;

function set_log_level($level) {

    global $log_level;

    if (is_string($level))
        $level = constant($level);

    if ($log_level != $level)
        stdout(W_INFO, "Setting log level: $level");

    $log_level = $level;

}

function stdout($level, $message, $job = NULL) {

    global $log_level;

    if ($level <= $log_level) {

        echo date('Y-m-d H:i:s') . ' - ';

        $consts = get_defined_constants(TRUE);

        $label = 'NONE';

        foreach($consts['user'] as $name => $value) {

            if (substr($name, 0, 2) == 'W_' && $value == $level) {

                $label = substr($name, 2);

                break;
            }

        }

        echo str_pad($label, 6, ' ', STR_PAD_LEFT) . ' - ' . ($job ? $job . ' - ' : '') . $message . "\n";

    }

}

class SocketClient {

    /*
     * WebSocket specific stuff
     */
    public $address;

    public $port;

    public $resource;

    public $socketCount = 0;

    public $closing = FALSE;

    // Buffer for fragmented frames
    public $frameBuffer = NULL;

    // Buffer for payloads split over multiple frames
    public $payloadBuffer = NULL;

    /*
     * Warlock specific stuff
     */
    public $id;

    public $type = 'client';  //Possible types are 'client', 'service' or 'admin'.

    public $username;

    public $since;

    public $status;

    /*
     * Any detected time offset. This doesn't need to be exact so we don't bother worrying about latency.
     */
    public $offset = NULL;

    public $admin = FALSE;

    public $subscriptions = array();
    // This is an array of event_id and socket pairs
    function __construct(&$server, $id, $type = 'client', $resource = NULL, $uid = NULL) {

        $allowed_types = array('client', 'service', 'admin');

        if(!in_array($type, $allowed_types))
            $type = 'client';

        $this->id = $id;

        $this->username = base64_decode($uid);

        $this->since = time();

        $this->type = $type;

        $this->resource = $resource;

        if ($this->username != NULL)
            stdout(W_NOTICE, "USER: $this->username");

        if (is_resource($this->resource)) {

            $resource_type = get_resource_type($this->resource);

            if ($resource_type == 'Socket')
                socket_getpeername($this->resource, $this->address, $this->port);

            stdout(W_NOTICE, "ADD: TYPE=$resource_type CLIENT=$this->id SOCKET=$this->resource");

        }

        $this->server = $server;

    }

    public function isLegacy() {

        return !is_resource($this->resource);

    }

    public function getType() {

        return $this->type;

    }

    public function isSubscribed($event_id) {

        return array_key_exists($event_id, $this->subscriptions);

    }

    public function subscribe($event_id, $resource = NULL) {

        if (!is_resource($resource)) {

            if (!$this->resource) {

                stdout(W_WARN, 'Subscription failed.  Bad socket resource!');

                return FALSE;
            }

            $resource = $this->resource;
        }

        stdout(W_NOTICE, "SUBSCRIBE: EVENT=$event_id CLIENT=$this->id RESOURCE=" . $resource);

        $this->subscriptions[$event_id] = $resource;

        return TRUE;

    }

    public function unsubscribe($event_id, $resource = NULL) {

        if (!array_key_exists($event_id, $this->subscriptions))
            return FALSE;

        if ($resource && !is_resource($resource))
            return FALSE;

        if ($resource && $resource != $this->subscriptions[$event_id])
            return FALSE;

        stdout(W_NOTICE, "UNSUBSCRIBE: EVENT=$event_id CLIENT=$this->id RESOURCE=" . $this->subscriptions[$event_id] . ' LEFT=' . count($this->subscriptions));

        unset($this->subscriptions[$event_id]);

        return TRUE;

    }

    public function sendEvent($event_id, $trigger_id, $data) {

        if (!array_key_exists($event_id, $this->subscriptions)) {

            stdout(W_WARN, "Client $this->id is not listening for $event_id");

            return FALSE;

        }

        $resource = $this->subscriptions[$event_id];

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(TRUE),
            'data' => $data
        );

        $result = $this->server->send($resource, 'event', $packet, $this->isLegacy());

        // Disconnect if we are a socket but not a websocket (legacy connection) and the result was negative.
        if (get_resource_type($resource) == 'Socket' && $this->isLegacy() && $result)
            $this->server->disconnect($resource);

        return TRUE;

    }

}

class Server extends WebSockets {

    /**
     * SERVER INSTANCE
     */
    private static $instance;

    private $silent = FALSE;

    private $running = NULL;
    // Main loop state. On false, Warlock will exit the main loop and terminate
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
    private $procs = array();
    // Currently running processes (jobs AND services)
    private $jobQueue = array();
    // Main job queue
    private $services = array();
    // Application services

    /**
     * QUEUES
     */
    private $waitQueue = array();
    // The wait queue. Clients subscribe to events and are added to this array.
    private $eventQueue = array();
    // The Event queue. Holds active events waiting to be seen.

    /**
     * SOCKETS & STREAMS
     */

    // The main socket for listening for incomming connections.
    private $master = NULL;

    // Currently connected sockets we are listening for data on.
    private $sockets = array();

    // Socket to client lookup table. Required as clients can have multiple connections.
    private $client_lookup = array();

    // Currently connected clients.
    private $clients = array();

    // The Warlock protocol encoder/decoder.
    private $protocol;

    function __construct($silent = FALSE) {

        parent::__construct(array(
            'warlock'
        ));

        global $STDOUT;

        global $STDERR;

        Server::$instance = $this;

        $this->silent = $silent;

        $defaults = array(
            'sys' => array(
                'id' => crc32(APPLICATION_PATH),
                'autostart' => FALSE,
                'pid' => 'warlock.pid',
                'cleanup' => TRUE,
                'timezone' => 'UTC'
            ),
            'server' => array(
                'listen' => '127.0.0.1',
                'port' => 8000,
                'encoded' => TRUE
            ),
            'timeouts' => array(
                'startup' => 1000,
                'listen' => 60
            ),
            'admin' => array(
                'trigger' => 'warlockadmintrigger',
                'key' => '0000'
            ),
            'log' => array(
                'level' => W_ERR,
                'file' => 'warlock.log',
                'error' => 'warlock-error.log',
                'rrd' => 'warlock.rrd'
            ),
            'job' => array(
                'retries' => 5,
                'retry' => 30,
                'expire' => 10
            ),
            'exec' => array(
                'timeout' => 30,
                'limit' => 5
            ),
            'service' => array(
                'restarts' => 5,
                'disable' => 300
            ),
            'event' => array(
                'queue_timeout' => 5
            )
        );

        $this->application = new \Hazaar\Application(APPLICATION_ENV);

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, $defaults);

        date_default_timezone_set($this->config->sys->timezone);

        set_log_level($this->config->log->level);

        if ($this->config->admin->key === '0000') {

            $msg = '* USING DEFAULT SYSTEM KEY!!!  PLEASE CONSIDER SETTING sys.key IN warlock config!!! *';

            stdout(W_WARN, str_repeat('*', strlen($msg)));

            stdout(W_WARN, $msg);

            stdout(W_WARN, str_repeat('*', strlen($msg)));
        }

        if ($this->silent) {

            if ($this->config->log->file) {

                fclose(STDOUT);

                $STDOUT = fopen($this->application->runtimePath($this->config->log->file), 'a');
            }

            if ($this->config->log->error) {

                fclose(STDERR);

                $STDERR = fopen($this->application->runtimePath($this->config->log->error), 'a');
            }
        }

        stdout(W_INFO, 'Warlock starting up...');

        $this->pid = getmypid();

        $this->pidfile = $this->application->runtimePath($this->config->sys->pid);

        if ($this->rrdfile = $this->application->runtimePath($this->config->log->rrd)) {

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

        stdout(W_NOTICE, 'Application path = ' . APPLICATION_PATH);

        stdout(W_NOTICE, 'Application name = ' . APPLICATION_NAME);

        stdout(W_NOTICE, 'Library path = ' . LIBRAY_PATH);

        stdout(W_NOTICE, 'Application environment = ' . APPLICATION_ENV);

        stdout(W_NOTICE, 'PID = ' . $this->pid);

        stdout(W_NOTICE, 'PID file = ' . $this->pidfile);

        stdout(W_DEBUG, 'Server ID = ' . $this->config->sys->id);

        stdout(W_DEBUG, 'Listen address = ' . $this->config->server->listen);

        stdout(W_DEBUG, 'Listen port = ' . $this->config->server->port);

        stdout(W_DEBUG, 'Job expiry = ' . $this->config->job->expire . ' seconds');

        stdout(W_DEBUG, 'Exec timeout = ' . $this->config->exec->timeout . ' seconds');

        stdout(W_DEBUG, 'Process limit = ' . $this->config->exec->limit . ' processes');

        $this->protocol = new \Hazaar\Application\Protocol($this->config->sys->id, $this->config->server->encoded);

    }

    public function shutdown($timeout = 0) {

        stdout(W_NOTICE, "SHUTDOWN: TIMEOUT=$timeout");

        $this->shutdown = time() + $timeout;

        $this->sendAdminEvent('shutdown');

        return TRUE;

    }

    function __destruct() {

        if (file_exists($this->pidfile))
            unlink($this->pidfile);

        stdout(W_INFO, 'Exiting...');

    }

    private function isRunning() {

        if (file_exists($this->pidfile)) {

            $pid = (int) file_get_contents($this->pidfile);

            if (file_exists('/proc/' . $pid)) {

                $this->pid = $pid;

                return TRUE;

            }

        }

        return FALSE;

    }

    public function bootstrap() {

        if ($this->isRunning()) {

            stdout(W_INFO, "Warlock is already running.");

            stdout(W_INFO, "Please stop the currently running instance first.");

            exit(1);

        }

        stdout(W_INFO, 'Creating TCP socket');

        $this->master = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if (!$this->master) {

            stdout(W_ERR, 'Unable to create AF_UNIX socket');

            return 1;

        }

        stdout(W_INFO, 'Configuring TCP socket');

        if (!socket_set_option($this->master, SOL_SOCKET, SO_REUSEADDR, 1)) {

            stdout(W_ERR, "Failed: socket_option()");

            return 1;

        }

        stdout(W_INFO, 'Binding to socket on ' . $this->config->server->listen . ':' . $this->config->server->port);

        if (!socket_bind($this->master, $this->config->server->listen, $this->config->server->port)) {

            stdout(W_ERR, 'Unable to bind to ' . $this->config->server->listen . ' on port ' . $this->config->server->port);

            return 1;

        }

        if (!socket_listen($this->master)) {

            stdout(W_ERR, 'Unable to listen on ' . $this->config->server->listen . ':' . $this->config->server->port);

            return 1;

        }

        $this->sockets[0] = $this->master;

        $this->running = TRUE;

        stdout(W_INFO, "Ready for connections...");

        return $this;

    }

    public function run() {

        $this->start = time();

        $services = new \Hazaar\Application\Config('service', APPLICATION_ENV);

        if ($services->loaded()) {

            stdout(W_INFO, "Checking for enabled services");

            foreach($services as $name => $options) {

                stdout(W_NOTICE, "Found service: $name");

                $this->services[$name] = $options->extend(array(
                    'name' => $name
                ))->toArray();

                if ($options['enabled'] === TRUE)
                    $this->serviceEnable($name, $options);

            }

        }

        file_put_contents($this->pidfile, $this->pid);

        $this->sendAdminEvent('startup', array(), TRUE);

        while($this->running) {

            if ($this->shutdown !== NULL && $this->shutdown <= time())
                $this->running = FALSE;

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

                            stdout(W_ERR, "Failed: socket_accept()");

                            continue;

                        } else {

                            $socket_id = intval($client_socket);

                            $this->sockets[$socket_id] = $client_socket;

                            socket_getpeername($client_socket, $address, $port);

                            stdout(W_INFO, "Connection from " . $address . ':' . $port);

                        }

                    } else {

                        $this->processClient($socket);

                    }

                }

            } else {

                $this->tv = 1;

            }

            $this->processJobs();

            $this->queueCleanup();

            $this->rrd->setValue('sockets', count($this->sockets));

            $this->rrd->setValue('clients', count($this->clients));

            $this->rrd->setValue('memory', memory_get_usage());

            $count = 0;

            foreach($this->services as $service) {

                if ($service['enabled'])
                    $count++;

            }

            $this->rrd->setValue('services', $count);

            $this->rrd->setValue('processes', count($this->procs));

            if ($this->rrd->update())
                gc_collect_cycles();

        }

        if (count($this->procs) > 0) {

            stdout(W_INFO, 'Terminating running processes');

            foreach($this->procs as $p) {

                stdout(W_NOTICE, "TERMINATE: PID=$p[pid]");

                $this->send($p['pipes'][0], 'cancel');

            }

            stdout(W_INFO, 'Waiting for processes to exit');

            $start = time();

            while(count($this->procs) > 0) {

                if ($start >= time() + 10) {

                    stdout(W_WARN, 'Timeout reached while waiting for process to exit.');

                    break;
                }

                $this->processJobs();

                if (count($this->procs) == 0)
                    break;

                sleep(1);

            }

        }

        stdout(W_INFO, 'Closing all connections');

        foreach($this->sockets as $socket)
            socket_close($socket);

        $this->sockets = array();

        stdout(W_INFO, 'Cleaning up');

        $this->procs = array();

        $this->jobQueue = array();

        $this->clients = array();

        $this->client_lookup = array();

        $this->eventQueue = array();

        $this->waitQueue = array();

        return 0;

    }

    public function disconnect($socket) {

        $socket_id = intval($socket);

        if ($client = $this->getClient($socket)) {

            stdout(W_NOTICE, 'DISCONNECT: CLIENT=' . $client->id . ' SOCKET=' . $socket . ' COUNT=' . $client->socketCount);

            $this->unsubscribe($client, NULL, $socket);

        }

        $this->removeClient($socket);

        /**
         * Remove the socket from our list of sockets
         */
        if (array_key_exists($socket_id, $this->sockets))
            unset($this->sockets[$socket_id]);

        stdout(W_NOTICE, "SOCKET_CLOSE: SOCKET=" . $socket);

        socket_close($socket);

    }

    private function addClient($id, $socket, $uid = NULL) {

        // If we don't have a socket or id, return FALSE
        if (!($socket && is_resource($socket) && $id))
            return FALSE;

        $socket_id = intval($socket);

        // If the socket already has a client lookup record, return FALSE
        if (array_key_exists($socket_id, $this->client_lookup))
            return FALSE;

        // If this client already exists, grab it otherwise create a new one
        if (array_key_exists($id, $this->clients)) {

            $client = $this->clients[$id];

        } else {

            $client = new SocketClient($this, $id, 'client', $socket, $uid);

            // Add it to the client array
            $this->clients[$id] = $client;

            $this->sendAdminEvent('add', array(
                'type' => 'client',
                'client' => array(
                    'id' => $client->id,
                    'username' => $client->username,
                    'since' => $client->since,
                    'ip' => $client->address,
                    'port' => $client->port,
                    'type' => $client->type,
                    'legacy' => $client->isLegacy()
                )
            ));

        }

        // Create a socket -> client lookup entry
        $this->client_lookup[$socket_id] = $id;

        return $client;

    }

    /**
     * Removes a client from a socket.
     *
     * Because a client can have multiple socket connections (in legacy mode) this removes the client reference
     * for that socket. Once there are no more references left the client is completely removed.
     *
     * @param
     *            $socket
     *
     * @return bool
     */
    private function removeClient($socket) {

        if (!$socket)
            return FALSE;

        $socket_id = intval($socket);

        if (!array_key_exists($socket_id, $this->client_lookup))
            return FALSE;

        $id = $this->client_lookup[$socket_id];

        if (!array_key_exists($id, $this->clients))
            return FALSE;

        $client = $this->clients[$id];

        unset($this->client_lookup[$socket_id]);

        $client->socketCount--;

        stdout(W_NOTICE, "LOOKUP_UNSET: CLIENT=$client->id SOCKETS=" . $client->socketCount);

        if (count($client->subscriptions) <= 0 && $client->socketCount <= 0) {

            stdout(W_NOTICE, "REMOVE: CLIENT=$id");

            unset($this->clients[$id]);

            $this->sendAdminEvent('remove', array(
                'type' => 'client',
                'client' => $id
            ));

            return TRUE;

        } else {

            stdout(W_NOTICE, "NOTREMOVE: SUBS=" . count($client->subscriptions) . ' SOCKETS=' . $client->socketCount);

        }

        return FALSE;

    }

    private function getClient($socket) {

        $socket_id = intval($socket);

        return (array_key_exists($socket_id, $this->client_lookup) ? $this->clients[$this->client_lookup[$socket_id]] : NULL);

    }

    private function processClient($socket) {

        $bytes_received = socket_recv($socket, $buf, 65536, 0);

        if ($bytes_received == 0) {

            stdout(W_NOTICE, 'Remote host closed connection');

            $this->disconnect($socket);

            return FALSE;

        }

        @$status = socket_getpeername($socket, $address, $port);

        if ($status == FALSE) {

            $this->disconnect($socket);

            return FALSE;

        }

        stdout(W_DEBUG, "SOCKET_RECV: HOST=$address:$port BYTES=" . strlen($buf));

        $client = $this->getClient($socket);

        if (!$client instanceof SocketClient) {

            $result = $this->initiateHandshake($socket, $buf);

            if (!$result)
                $this->disconnect($socket);

        } else {

            /**
             * Sometimes we can get multiple frames in a single buffer so we cycle through them until they are all processed.
             * This will even allow partial frames to be added to the client frame buffer.
             */
            while($frame = $this->processFrame($buf, $client)) {

                stdout(W_DECODE, "RECV_PACKET: " . $frame);

                if ($type = $this->protocol->decode($frame, $payload, $time)) {

                    $client->offset = (time() - $time);

                    if (!$this->processCommand($socket, $client, $type, $payload, $time)) {

                        stdout(W_ERR, 'An error occurred processing the command TYPE: ' . $type);

                        $this->send($socket, 'error', array(
                            'reason' => 'An error occurred processing the command',
                            'command' => $this->protocol->getTypeName($type)
                        ));

                    }

                } else {

                    $reason = $this->protocol->getLastError();

                    stdout(W_ERR, "Protocol error: $reason");

                    $this->send($socket, $this->protocol->encode('error', array(
                        'reason' => $reason
                    )));

                    continue;

                }

            }

        }

        return TRUE;

    }

    private function httpResponse($code, $body = NULL, $headers = array()) {

        if (!is_array($headers))
            return FALSE;

        $lf = "\r\n";

        $response = "HTTP/1.1 $code " . http_response_text($code) . $lf;

        $defaultHeaders = array(
            'Date' => date('r'),
            'Server' => 'Warlock/2.0 (' . php_uname('s') . ')',
            'X-Powered-By' => phpversion()
        );

        if ($body)
            $defaultHeaders['Content-Length'] = strlen($body);

        $headers = array_merge($defaultHeaders, $headers);

        foreach($headers as $key => $value)
            $response .= $key . ': ' . $value . $lf;

        return $response . $lf . $body;

    }

    private function initiateHandshake($socket, $request) {

        if(!($headers = $this->parseHeaders($request))){

            stdout(W_WARN, 'Unable to parse request while initiating WebSocket handshake!');

            return false;

        }

        $url = NULL;

        $responseCode = FALSE;

        if (array_key_exists('connection', $headers) && preg_match('/upgrade/', strtolower($headers['connection']))) {

            if (array_key_exists('get', $headers) && ($responseCode = $this->acceptHandshake($headers, $responseHeaders, NULL, $results)) == 101) {

                stdout(W_DEBUG, "Initiating WebSockets handshake");

                if (!($cid = $results['url']['CID']))
                    return FALSE;

                $client = $this->addClient($cid, $socket, (array_key_exists('UID', $results['url']) ? $results['url']['UID'] : NULL));

                $client->socketCount++;

                $response = $this->httpResponse($responseCode, NULL, $responseHeaders);

                socket_write($socket, $response, strlen($response));

                stdout(W_DEBUG, 'WebSockets handshake successful!');

                return TRUE;

            } elseif ($responseCode > 0) {

                $responseHeaders['Connection'] = 'close';

                $responseHeaders['Content-Type'] = 'text/text';

                $body = $responseCode . ' ' . http_response_text($responseCode);

                $response = $this->httpResponse($responseCode, $body, $responseHeaders);

                stdout(W_WARN, "Handshake failed with code $responseCode");

                socket_write($socket, $response, strlen($response));

            }

        } else {

            /**
             * Legacy long polling handshake
             */

            stdout(W_INFO, "Processing legacy request");

            if (array_key_exists('get', $headers))
                $url = parse_url($headers['get']);
            elseif (array_key_exists('post', $headers))
                $url = parse_url($headers['post']);

            if (array_key_exists('query', $url)) {

                $queryString = $url['query'];

            } else {

                if ($offset = strpos($request, "\r\n\r\n") + 4)
                    $queryString = substr($request, $offset);
                else
                    return FALSE;

            }

            parse_str($queryString, $query);

            if (!(array_key_exists('CID', $query) && array_key_exists('P', $query)))
                return FALSE;

            if (array_key_exists($query['CID'], $this->clients)) {

                $client = $this->clients[$query['CID']];

                $client->socketCount++;

                stdout(W_DEBUG, "FOUND: CLIENT=$client->id SOCKET=$socket COUNT=$client->socketCount");

            } else {

                $client = new SocketClient($this, $query['CID'], 'client', null, (array_key_exists('UID', $query) ? $query['UID'] : null));

                $client->socketCount = 1;

                socket_getpeername($socket, $client->address, $client->port);

                $this->clients[$query['CID']] = $client;

                stdout(W_NOTICE, "ADD: CLIENT=$client->id MODE=legacy SOCKET=$socket COUNT=$client->socketCount");

                $this->sendAdminEvent('add', array(
                    'type' => 'client',
                    'client' => array(
                        'id' => $client->id,
                        'username' => $client->username,
                        'since' => $client->since,
                        'ip' => $client->address,
                        'port' => $client->port,
                        'type' => $client->type,
                        'legacy' => $client->isLegacy()
                    )
                ));

            }

            $socket_id = intval($socket);

            $this->client_lookup[$socket_id] = $client->id;

            $responseHeaders['Connection'] = 'Keep-alive';

            $responseHeaders['Access-Control-Allow-Origin'] = $headers['origin'];

            $responseHeaders['Content-Type'] = 'text/text';

            $type = $this->protocol->decode($query['P'], $payload);

            if ($type) {

                $response = $this->httpResponse(200, NULL, $responseHeaders);

                socket_write($socket, $response, strlen($response));

                $client->handshake = TRUE;

                $result = $this->processCommand($socket, $client, $type, $payload);

                return !$result; // If $result is TRUE then we disconnect. If it is FALSE we do NOT disconnect.

            } else {

                $reason = $this->protocol->getLastError();

                stdout(W_ERR, "Connection rejected - $reason");

                $response = $this->httpResponse(200, $this->protocol->encode('error', array(
                    'reason' => $reason
                )), $responseHeaders);

                socket_write($socket, $response, strlen($response));

            }

        }

        return FALSE; // FALSE means disconnect

    }

    protected function checkRequestURL($url) {

        $parts = parse_url($url);

        // Check that a path was actually sent
        if (!array_key_exists('path', $parts))
            return FALSE;

        // Check that the path is correct based on the APPLICATION_NAME constant
        if ($parts['path'] != '/' . APPLICATION_NAME . '/warlock')
            return FALSE;

        // Check to see if there is a query part as this should contain the CID
        if (!array_key_exists('query', $parts))
            return FALSE;

        // Get the CID
        parse_str($parts['query'], $query);

        if (!array_key_exists('CID', $query))
            return FALSE;

        return $query;

    }

    private function processFrame(&$frameBuffer, &$client) {

        if ($client->frameBuffer) {

            $frameBuffer = $client->frameBuffer . $frameBuffer;

            $client->frameBuffer = NULL;

            return $this->processFrame($frameBuffer, $client);

        }

        if (!$frameBuffer)
            return FALSE;

        stdout(W_DECODE, "RECV_FRAME: " . implode(' ', $this->hexString($frameBuffer)));

        $opcode = $this->getFrame($frameBuffer, $payload);

        /**
         * If we get an opcode that equals FALSE then we got a bad frame.
         *
         * If we get a opcode of -1 there are more frames to come for this payload. So, we return FALSE if there are no
         * more frames to process, or TRUE if there are already more frames in the buffer to process.
         */
        if ($opcode === FALSE) {

            stdout(W_ERR, 'Bad frame received from client. Disconnecting.');

            $this->disconnect($client->socket);

            return FALSE;

        } elseif ($opcode === -1) {

            $client->payloadBuffer .= $payload;

            return (strlen($frameBuffer) > 0);

        }

        stdout(W_DECODE, "OPCODE: $opcode");

        switch ($opcode) {

            case 0 :
            case 1 :
            case 2 :

                break;

            case 8 :

                if($client->closing === false){

                    stdout(W_DEBUG, "WEBSOCKET_CLOSE: HOST=$client->address:$client->port");

                    $client->closing = TRUE;

                    $frame = $this->frame('', 'close', FALSE);

                    socket_write($client->resource, $frame, strlen($frame));

                    $this->disconnect($client->resource);

                }

                return FALSE;

            case 9 :

                stdout(W_DEBUG, "WEBSOCKET_PING: HOST=$client->address:$client->port");

                $frame = $this->frame('', 'pong', FALSE);

                socket_write($client->resource, $frame, strlen($frame));

                return FALSE;

            case 10 :

                stdout(W_DEBUG, "WEBSOCKET_PONG: HOST=$client->address:$client->port");

                return FALSE;

            default :

                $this->disconnect($client->resource);

                return FALSE;

        }

        if (strlen($frameBuffer) > 0) {

            $client->frameBuffer = $frameBuffer;

            $frameBuffer = '';

        }

        if ($client->payloadBuffer) {

            $payload = $client->payloadBuffer . $payload;

            $client->payloadBuffer = '';

        }

        return $payload;

    }

    public function send($resource, $command, $payload = NULL, $is_legacy = false, $time = null) {

        if (!is_resource($resource))
            return FALSE;

        if (!is_string($command))
            return FALSE;

        $packet = $this->protocol->encode($command, $payload, $time); //Override the timestamp.  Only used for PING/PONG really.

        stdout(W_DECODE, "SEND_PACKET: $packet");

        if ($is_legacy) {

            $frame = $packet;

        } else {

            $frame = $this->frame($packet, 'text', FALSE);

            stdout(W_DECODE, "SEND_FRAME: " . implode(' ', $this->hexString($frame)));

        }

        $len = strlen($frame);

        stdout(W_DEBUG, "SOCKET_WRITE: BYTES=$len SOCKET=$resource LEGACY=" . ($is_legacy ? 'TRUE' : 'FALSE'));

        $bytes_sent = socket_write($resource, $frame, $len);

        if ($bytes_sent == -1) {

            stdout(W_ERR, 'An error occured while sending to the client');

            return FALSE;

        } elseif ($bytes_sent != $len) {

            stdout(W_ERR, $bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected');

            return FALSE;

        }


        return TRUE;

    }

    private function getStatus($full = TRUE) {

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
                'type' => $client->type,
                'legacy' => $client->isLegacy()
            );
        }

        $arrays = array(
            // Main job queue
            'queue' => array(
                &$this->jobQueue,
                array(
                    'function',
                    'client'
                )
            ),
            // Active process queue
            'processes' => array(
                &$this->procs,
                array(
                    'pipes',
                    'process'
                )
            ),
            // Configured services
            'services' => array(
                &$this->services
            ),
            // Event queue
            'events' => array(
                &$this->eventQueue
            )
        );

        foreach($arrays as $name => $array) {

            $count = (is_array($array) ? count($array[0]) : count($array));

            $status['stats'][$name] = $count;

            if ($count > 0) {

                if (isset($array[1])) {

                    $bad_keys = $array[1];

                    foreach($array[0] as $id => $item)
                        $status[$name][$id] = array_diff_key($item, array_flip($bad_keys));

                } else {

                    if ($name == 'events' && array_key_exists($this->config->admin->trigger, $array[0])) {

                        $status[$name] = array_diff_key($array[0], array_flip(array(
                            $this->config->admin->trigger
                        )));

                    } else {

                        $status[$name] = $array[0];

                    }

                }

            }

        }

        return $status;

    }

    private function processCommand($resource, &$client, $command, &$payload, $time) {

        if (!$command)
            return FALSE;

        $type = $client->isLegacy() ? 'Legacy' : 'WebSocket';

        stdout(W_NOTICE, "COMMAND: $command" . ($client->id ? " CLIENT=$client->id" : NULL) . " TYPE=$type");

        switch ($command) {

            case 'NOOP':

                stdout(W_DEBUG, 'NOOP: ' . $payload);

                return true;

            case 'SYNC':

                return $this->commandSync($resource, $client, $payload);

            case 'OK':

                stdout(W_WARN, "Received OK, but I don't know why!");

                break;

            case 'ERROR':

                stdout(W_ERR, $payload);

                break;

            case 'STATUS' :

                return $this->commandStatus($resource, $client, $payload);

            case 'SHUTDOWN' :

                return $this->commandStop($resource, $client);

            case 'DELAY' :

                return $this->commandDelay($resource, $client, $payload);

            case 'SCHEDULE' :

                return $this->commandSchedule($resource, $client, $payload);

            case 'CANCEL' :

                if (array_key_exists('job_id', $payload))
                    return $this->commandCancel($resource, $client, $payload['job_id']);

                break;

            case 'ENABLE' :

                return $this->commandEnable($resource, $client, $payload);

            case 'DISABLE' :

                return $this->commandDisable($resource, $client, $payload);

            case 'SERVICE' :

                if (array_key_exists('name', $payload))
                    return $this->commandService($resource, $client, $payload);

                break;

            case 'SUBSCRIBE' :

                $filter = (array_key_exists('filter', $payload) ? $payload['filter'] : NULL);

                return $this->commandSubscribe($resource, $client, $payload['id'], $filter);

            case 'UNSUBSCRIBE' :

                return $this->commandUnsubscribe($resource, $client, $payload['id']);

            case 'TRIGGER' :

                $data = (array_key_exists('data', $payload) ? $payload['data'] : NULL);

                return $this->commandTrigger($resource, $client, $payload['id'], $data);

            case 'PING' :

                return $this->send($resource, 'pong', $payload);

            case 'PONG':

                $trip_ms = (microtime(true) - $payload) * 1000;

                stdout(W_INFO, 'PONG received in ' . $trip_ms . 'ms');

                break;

            case 'DEBUG':

                stdout(W_DEBUG, $payload);

                return true;

        }

        return FALSE;

    }

    private function commandSync($resource, &$client, $payload){

        stdout(W_NOTICE, "SYNC: CLIENT_ID=$client->id OFFSET=$client->offset");

        print_r($payload); exit;

        if (is_array($payload)
            && array_key_exists('admin_key', $payload)
            && $payload['admin_key'] === $this->config->admin->key) {

            stdout(W_INFO, 'Warlock control authorised to ' . $client->id);

            $client->admin = TRUE;

            $this->send($resource, 'OK', NULL, $client->isLegacy());

            $this->sendAdminEvent('update', array(
                'type' => 'client',
                'client' => array(
                    'id' => $client->id,
                    'admin' => $client->admin
                )
            ));

        }elseif(is_array($payload)
            && array_key_exists('access_key', $payload)
            && $payload['access_key'] === $client->id){

            stdout(W_INFO, 'Service connected successfully');

            $client->system = TRUE;

            $this->send($resource, 'OK', NULL, $client->isLegacy());

        } else {

            stdout(W_ERR, 'Client requested bad sync!');

            $client->closing = TRUE;

        }

        return TRUE;

    }

    private function commandStop($resource, &$client) {

        if (!$client->admin)
            return FALSE;

        stdout(W_INFO, "Shutdown requested");

        $this->send($resource, 'ok', NULL, $client->isLegacy());

        return $this->shutdown(1);

    }

    private function commandStatus($resource, &$client, $payload = null) {

        if(!$payload){

            if (!$client->type == 'admin')
                return FALSE;

            return $this->send($resource, 'status', $this->getStatus(), $client->isLegacy());

        }

        return true;

    }

    private function commandDelay($resource, &$client, $command) {

        if (!$client->admin)
            return FALSE;

        if ($command['value'] == NULL)
            $command['value'] = 0;

        $when = time() + $command['value'];

        $tag = NULL;

        $tag_overwrite = FALSE;

        if (array_key_exists('tag', $command)) {

            $tag = $command['tag'];

            $tag_overwrite = $command['overwrite'];

        }

        if ($id = $this->scheduleJob($when, $command['function'], $command['application'], $tag, $tag_overwrite)) {

            stdout(W_INFO, "Successfully scheduled delayed function.  Executing in $command[value] seconds . ", $id);

            return $this->send($resource, 'ok', array(
                'job_id' => $id
            ), $client->isLegacy());

        }

        stdout(W_ERR, "Could not schedule delayed function");

        return $this->send($resource, 'error', NULL, $client->isLegacy());

    }

    private function commandSchedule($resource, &$client, $command) {

        if (!$client->admin)
            return FALSE;

        $tag = NULL;

        $tag_overwrite = FALSE;

        if (array_key_exists('tag', $command)) {

            $tag = $command['tag'];

            $tag_overwrite = $command['overwrite'];

        }

        if ($id = $this->scheduleJob($command['when'], $command['code'], $command['application'], $tag, $tag_overwrite)) {

            stdout(W_INFO, "Function execution successfully scheduled", $id);

            return $this->send($resource, 'ok', array(
                'job_id' => $id
            ), $client->isLegacy());

        }

        stdout(W_ERR, "Could not schedule function");

        return $this->send($resource, 'error', NULL, $client->isLegacy());

    }

    private function commandCancel($resource, &$client, $job_id) {

        if (!$client->admin)
            return FALSE;

        if ($this->cancelJob($job_id)) {

            stdout(W_NOTICE, "Job successfully canceled");

            return $this->send($resource, 'ok', NULL, $client->isLegacy());

        }

        stdout(W_ERR, 'Error trying to cancel job');

        return $this->send($resource, 'error', NULL, $client->isLegacy());

    }

    private function commandSubscribe($resource, &$client, $event_id, $filter = NULL) {

        if (!$this->subscribe($resource, $client, $event_id, $filter))
            return FALSE;

        /*
         * Check to see if this subscribe request has any active and unseen events waiting for it.
         *
         * If we get a TRUE back we need to carry on
         */
        if (!$this->processEventQueue($client, $event_id, $filter))
            return $client->isLegacy();

        // If we are long polling, return false so we don't disconnect

        if ($client->isLegacy())
            return FALSE;

        $this->send($resource, 'ok', NULL, $client->isLegacy());

        return TRUE;

    }

    private function commandUnsubscribe($resource, &$client, $event_id) {

        if ($this->unSubscribe($client, $event_id))
            return $this->send($resource, 'ok', NULL, $client->isLegacy());

        return $this->send($resource, 'error', NULL, $client->isLegacy());

    }

    private function commandTrigger($resource, &$client, $event_id, $data) {

        $this->stats['events']++;

        $this->rrd->setValue('events', 1);

        $trigger_id = uniqid();

        $this->eventQueue[$event_id][$trigger_id] = $new = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'when' => time(),
            'data' => $data
        );

        if ($event_id != $this->config->admin->trigger) {

            $this->sendAdminEvent('add', array(
                'type' => 'event',
                'event' => $new
            ));
        }

        // Check to see if there are any clients waiting for this event and send notifications to them all.
        $this->processSubscriptionQueue($event_id, $trigger_id);

        stdout(W_NOTICE, "EVENT_QUEUE: NAME=$event_id COUNT=" . count($this->eventQueue[$event_id]));

        if($client instanceof Client){

            stdout(W_NOTICE, "TRIGGER: NAME=$event_id CLIENT=$client->id");

            $this->send($resource, 'ok', NULL, $client->isLegacy());

        }

        return TRUE;

    }

    private function commandEnable($resource, &$client, $name) {

        if (!$client->admin)
            return FALSE;

        $result = $this->serviceEnable($name);

        $this->send($resource, ($result ? 'ok' : 'error'), NULL, $client->isLegacy());

        return TRUE;

    }

    private function commandDisable($resource, &$client, $name) {

        if (!$client->admin)
            return FALSE;

        $result = $this->serviceDisable($name);

        $this->send($resource, ($result ? 'ok' : 'error'), NULL, $client->isLegacy());

        return TRUE;

    }

    private function commandService($resource, &$client, $name) {

        if (!$client->admin)
            return FALSE;

        stdout(W_WARN, 'Request service status not completed yet');

        return FALSE;

    }

    public function subscribe($resource, &$client, $event_id, $filter) {

        if ($client->isSubscribed($event_id))
            return FALSE;

        $new = array(
            'client' => $client->id,
            'since' => time(),
            'filter' => $filter
        );

        $this->waitQueue[$event_id][] = $new;

        $client->subscribe($event_id, $resource);

        if ($event_id == $this->config->admin->trigger) {

            stdout(W_NOTICE, "ADMIN_SUBSCRIBE: CLIENT=$client->id");

        } else {

            $this->stats['subscriptions']++;

            $this->sendAdminEvent('add', array(
                'type' => 'subscription',
                'event' => $event_id,
                'data' => $new
            ));
        }

        return TRUE;

    }

    public function unsubscribe(&$client, $event_id = NULL, $resource = NULL) {

        if ($event_id === NULL) {

            foreach($client->subscriptions as $event_id => $event_resource) {

                // If we have a socket, only unsubscribe events subscribed on that socket
                if (is_resource($resource) && $resource != $event_resource)
                    continue;

                $this->unsubscribe($client, $event_id, $event_resource);
            }

        } else {

            if ($client->unsubscribe($event_id, $resource)) {

                if (array_key_exists($event_id, $this->waitQueue) && is_array($this->waitQueue[$event_id])) {

                    foreach($this->waitQueue[$event_id] as $id => $item) {

                        if ($item['client'] == $client->id) {

                            stdout(W_NOTICE, "DEQUEUE: NAME=$event_id CLIENT=$client->id");

                            unset($this->waitQueue[$event_id][$id]);

                            if ($event_id == $this->config->admin->trigger) {

                                stdout(W_NOTICE, "ADMIN_UNSUBSCRIBE: CLIENT=$client->id");

                            } else {

                                $this->stats['subscriptions']--;

                                $this->sendAdminEvent('remove', array(
                                    'type' => 'subscription',
                                    'event' => $event_id,
                                    'subscription' => array(
                                        'client' => $client->id
                                    )
                                ));

                            }

                        }

                    }

                }

            }

        }

        return TRUE;

    }

    /*
     * This method simple increments the jids integer but makes sure it is unique before returning it.
     */
    public function getJobId() {

        $count = 0;

        $jid = NULL;

        while(array_key_exists($jid = uniqid(), $this->jobQueue)) {

            if ($count >= 10) {

                stdout(W_ERR, "Unable to generate job ID after $count attempts . Giving up . This is bad! ");

                return FALSE;

            }

        }

        return $jid;

    }

    private function scheduleJob($when, $function, $application, $tag = NULL, $overwrite = FALSE) {

        if (!($id = $this->getJobId()))
            return FALSE;

        stdout(W_DEBUG, "JOB: ID=$id");

        if (!is_numeric($when)) {

            stdout(W_DEBUG, 'Parsing string time', $id);

            $when = strtotime($when);

        }

        stdout(W_DEBUG, 'NOW:  ' . date('c'), $id);

        stdout(W_DEBUG, 'WHEN: ' . date('c', $when), $id);

        stdout(W_DEBUG, 'APPLICATION_PATH: ' . $application['path'], $id);

        stdout(W_DEBUG, 'APPLICATION_ENV:  ' . $application['env'], $id);

        if (!$when || $when < time()) {

            stdout(W_WARN, 'Trying to schedule job to execute in the past', $id);

            return FALSE;

        }

        stdout(W_DEBUG, 'Scheduling job for execution at ' . date('c', $when), $id);

        if ($tag) {

            stdout(W_DEBUG, 'TAG: ' . $tag, $id);

            if (array_key_exists($tag, $this->tags)) {

                stdout(W_DEBUG, "Job already scheduled with tag $tag", $id);

                if ($overwrite == 'true') {

                    $id = $this->tags[$tag];

                    stdout(W_DEBUG, 'Overwriting', $id);

                } else {

                    stdout(W_DEBUG, 'Skipping', $id);

                    return FALSE;

                }

            }

            $this->tags[$tag] = $id;

        }

        $params = array();

        if (array_key_exists('params', $function)) {

            foreach($function['params'] as $p)
                $params[] = (string) $p;

        }

        $this->jobQueue[$id] = array(
            'id' => $id,
            'start' => $when,
            'type' => 'job',
            'application' => array(
                'path' => $application['path'],
                'env' => $application['env']
            ),
            'function' => $function['code'],
            'params' => $params,
            'tag' => $tag,
            'status' => STATUS_QUEUED,
            'status_text' => 'queued',
            'retries' => 0,
            'expire' => 0
        );

        stdout(W_DEBUG, 'Job added to queue', $id);

        $this->stats['queue']++;

        $bad_keys = array(
            'function'
        );

        $this->sendAdminEvent('add', array(
            'type' => 'job',
            'id' => $id,
            'job' => array_diff_key($this->jobQueue[$id], array_flip($bad_keys))
        ));

        return $id;

    }

    private function cancelJob($job_id) {

        stdout(W_DEBUG, 'Trying to cancel job', $job_id);

        if (!array_key_exists($job_id, $this->jobQueue) || $this->jobQueue[$job_id]['status'] != STATUS_QUEUED || $this->jobQueue[$job_id]['status'] != STATUS_QUEUED_RETRY) {

            return FALSE;

        }

        if (array_key_exists('tag', $this->jobQueue[$job_id]))
            unset($this->tags[$this->jobQueue[$job_id]['tag']]);

        /**
         * Stop the job if it is currently running
         */
        if ($this->jobQueue[$job_id]['status'] == STATUS_RUNNING) {

            $type = $this->jobQueue[$job_id]['type'];

            if (array_key_exists($job_id, $this->procs)) {

                stdout(W_DEBUG, 'Stopping running ' . $type);

                proc_close($this->procs[$job_id]['process']);

            } else {

                stdout(W_ERR, ucfirst($type) . ' has running status but proccess resource was not found!');

            }

        }

        $this->setJobStatus($job_id, STATUS_CANCELLED);

        // Expire the job in 30 seconds
        $this->jobQueue[$job_id]['expire'] = time() + $this->config->job->expire;

        return TRUE;

    }

    private function setJobStatus($job_id, $status) {

        if (array_key_exists($job_id, $this->jobQueue)) {

            $this->jobQueue[$job_id]['status'] = $status;

            $this->jobQueue[$job_id]['status_text'] = $this->getJobStatus($job_id);

            stdout(W_NOTICE, $job_id . ' - ' . strtoupper($this->jobQueue[$job_id]['status_text']));

            $this->sendAdminEvent('update', array(
                'type' => 'job',
                'id' => $job_id,
                'job' => $this->jobQueue[$job_id]
            ));

        }

    }

    private function getJobStatus($job_id) {

        if ($this->config->event->queue_timeout)
            if (array_key_exists($job_id, $this->jobQueue)) {

                switch ($this->jobQueue[$job_id]['status']) {

                    case STATUS_QUEUED :
                        $ret = 'queued';
                        break;

                    case STATUS_QUEUED_RETRY :
                        $ret = 'queued (retrying)';
                        break;

                    case STATUS_STARTING :
                        $ret = 'starting';
                        break;

                    case STATUS_RUNNING :
                        $ret = 'running';
                        break;

                    case STATUS_COMPLETE :
                        $ret = 'completed';
                        break;

                    case STATUS_CANCELLED :
                        $ret = 'cancelled';
                        break;

                    case STATUS_ERROR :
                        $ret = 'error';
                        break;

                    default :
                        $ret = 'invalid';
                        break;
                }

                return $ret;
            }

        return NULL;

    }

    /*
     * Main job processor loop This is the main loop that executed scheduled jobs.
     * It uses proc_open to execute jobs in their own process so
     * that they don't interfere with other scheduled jobs.
     */
    private function processJobs() {

        /*
         * Check the queue for any jobs to run
         */
        foreach($this->jobQueue as $id => &$job) {

            try{

                $now = time();

                if (($job['status'] == STATUS_QUEUED || $job['status'] == STATUS_QUEUED_RETRY) && $now >= $job['start']) {

                    if (count($this->procs) >= $this->config->exec->limit) {

                        $this->stats['limitHits']++;

                        stdout(W_WARN, 'Process limit of ' . $this->config->exec->limit . ' processes reached!');

                        break;

                    }

                    if ($job['type'] != 'service') {

                        if ($job['retries'] > 0)
                            $this->stats['retries']++;

                        $this->rrd->setValue('jobs', 1);

                        stdout(W_INFO, "Starting job execution", $id);

                        stdout(W_NOTICE, 'NOW:  ' . date('c', $now), $id);

                        stdout(W_NOTICE, 'WHEN: ' . date('c', $job['start']), $id);

                        if ($job['retries'] > 0)
                            stdout(W_DEBUG, 'RETRIES: ' . $job['retries'], $id);

                        $late = $now - $job['start'];

                        $job['late'] = $late;

                        if ($late > 0) {

                            stdout(W_DEBUG, "LATE: $late seconds", $id);

                            $this->stats['lateExecs']++;

                        }

                        if (array_key_exists('params', $job) && count($job['params']) > 0)
                            stdout(W_NOTICE, 'PARAMS: ' . implode(', ', $job['params']), $id);

                    }

                    /*
                     * Open a new process to php CLI and pipe the function out to it for execution
                     */
                    $descriptorspec = array(
                        0 => array('pipe', 'r'),
                        1 => array('pipe', 'w'),
                        2 => array('pipe', 'w')
                    );

                    $env = array_filter(array_merge($_SERVER, array(
                        'APPLICATION_PATH' => $job['application']['path'],
                        'APPLICATION_ENV' => $job['application']['env'],
                        'HAZAAR_ADMIN_KEY' => $this->config->admin->key,
                        'HAZAAR_SID' => $this->config->sys->id,
                        'USERNAME' => $_SERVER['USERNAME']
                    )), 'is_string');

                    $cmd = realpath(LIBRAY_PATH . '/Runner.php');

                    if (!$cmd || !file_exists($cmd))
                        throw new \Exception('Application command runner could not be found!');

                    $php_binary = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) == 'WIN')?'.exe':'');

                    if (!file_exists($php_binary))
                        throw new \Exception('The PHP CLI binary does not exist at ' . $php_binary);

                    if (!is_executable($php_binary))
                        throw new \Exception('The PHP CLI binary exists but is not executable!');

                    $proc_cmd = basename($php_binary) . ' "' . $cmd . '"';

                    stdout(W_DEBUG, 'Exec: ' . $proc_cmd);

                    $process = proc_open($proc_cmd, $descriptorspec, $pipes, dirname($php_binary), $env);

                    if (is_resource($process)) {

                        $status = proc_get_status($process);

                        stdout(W_NOTICE, 'PID: ' . $status['pid'], $id);

                        $this->stats['processed']++;

                        $this->setJobStatus($id, STATUS_RUNNING);

                        $output = '';

                        $payload = array(
                            'application_name' => APPLICATION_NAME,
                            'server_port' => $this->config->server['port'] ,
                            'job_id' => $id,
                            'access_key' => $job['access_key'] = uniqid()
                        );

                        if ($job['type'] == 'service') {

                            $name = $job['service'];

                            $this->services[$name]['status'] = 'running';

                            $this->sendAdminEvent('update', array(
                                'type' => 'service',
                                'service' => $this->services[$name]
                            ));

                            $payload['name'] = $job['service'];

                            $output = $this->protocol->encode('service', $payload);

                        } elseif ($job['type'] == 'job') {

                            $payload['function'] = $job['function'];

                            if (array_key_exists('params', $job) && is_array($job['params']) && count($job['params']) > 0)
                                $payload['params'] = $job['params'];

                            $output = $this->protocol->encode('exec', $payload);

                        }

                        fwrite($pipes[0], $output);

                        fclose($pipes[0]); //Close the pipe to signal the end of the input

                        $this->procs[$id] = array(
                            'id' => $id,
                            'type' => $job['type'],
                            'start' => time(),
                            'pid' => $status['pid'],
                            'tag' => $job['tag'],
                            'env' => $job['application']['env'],
                            'process' => $process,
                            'pipes' => $pipes
                        );

                        $this->stats['processes']++;

                        $bad_keys = array(
                            'process',
                            'pipes'
                        );

                        $this->sendAdminEvent('add', array(
                            'type' => 'process',
                            'id' => $id,
                            'process' => array_diff_key($this->procs[$id], array_flip($bad_keys))
                        ));

                    } else {

                        $this->setJobStatus($id, STATUS_ERROR);

                        $job['expire'] = time() + $this->config->job->expire;
                        // Expire the job when the queue expiry is reached

                        stdout(W_ERR, 'Could not create child process.  Execution failed', $id);

                    }

                    if (array_key_exists('tag', $job) && $job['tag'])
                        unset($this->tags[$job['tag']]);

                } elseif ($job['status'] == STATUS_CANCELLED) {

                    $job['expire'] = time() + $this->config->job->expire;

                    $this->setJobStatus($id, STATUS_COMPLETE);

                } elseif ($job['status'] == STATUS_ERROR || ($job['status'] == STATUS_COMPLETE && $job['expire'] > 0)) {

                    if (!array_key_exists('expire', $job) || time() >= $job['expire']) {

                        stdout(W_NOTICE, 'Cleaning up', $id);

                        $this->stats['queue']--;

                        $this->sendAdminEvent('remove', array(
                            'type' => 'job',
                            'id' => $id
                        ));

                        if ($job['type'] == 'service')
                            unset($this->services[$job['service']]['job']);

                        unset($this->jobQueue[$id]);

                    }

                }

            }
            catch(\Exception $e){

                stdout(W_ERR, $e->getMessage());

                $job['status'] = STATUS_ERROR;

            }

        }

        /*
         * Check running processes
         */
        foreach($this->procs as $id => &$proc) {

            if (!array_key_exists($id, $this->jobQueue)) {

                stdout(W_ERR, 'Process found that has no job queue entry!');

                proc_terminate($proc['process']);

                unset($this->procs[$id]);

                continue;

            }

            $job = & $this->jobQueue[$id];

            $status = proc_get_status($proc['process']);

            if ($status['running'] === FALSE) {

                $this->stats['processes']--;

                unset($this->procs[$id]);

                $this->sendAdminEvent('remove', array(
                    'type' => 'process',
                    'id' => $id
                ));

                /**
                 * Process a Service exit
                 */
                if ($proc['type'] == 'service' && $job['status'] != STATUS_CANCELLED) {

                    $name = $job['service'];

                    $this->services[$name]['status'] = 'stopped';

                    $this->sendAdminEvent('update', array(
                        'type' => 'service',
                        'service' => $this->services[$name]
                    ));

                    stdout(W_NOTICE, "SERVICE=$name EXIT=$status[exitcode]");

                    foreach($proc['pipes'] as $sid => $pipe) {

                        //Skip the STDIN pipe for this process
                        if($sid == 0)
                            continue;

                        if ($input = stream_get_contents($pipe))
                            echo $input;

                        fclose($pipe);

                    }

                    if ($status['exitcode'] > 0) {

                        stdout(W_ERR, "Service '$name' returned status code $status[exitcode]");

                        if ($status['exitcode'] == 2) {

                            stdout(W_ERR, 'Service failed to start because service class does not exist.  Disabling service.');

                            $this->setJobStatus($id, STATUS_ERROR);

                            continue;

                        } elseif ($status['exitcode'] == 3) {

                            stdout(W_ERR, 'Service failed to start because it has missing required modules.  Disabling service.');

                            $this->setJobStatus($id, STATUS_ERROR);

                            continue;

                        }

                        $job['retries']++;

                        $this->setJobStatus($id, STATUS_QUEUED_RETRY);

                        if ($job['retries'] > $this->config->service->restarts) {

                            stdout(W_INFO, "Service '$name' is restarting too often.  Disabling for {$this->config->service->disable} seconds.");

                            $job['start'] = time() + $this->config->service->disable;

                            $job['retries'] = 0;

                            $job['expire'] = 0;

                        } else {

                            stdout(W_INFO, "Restarting service '$name'. ({$job['retries']})");

                        }

                    } elseif ($job['respawn'] == TRUE && $job['status'] == STATUS_RUNNING) {

                        stdout(W_INFO, "Respawning service '$name' in " . $job['respawn_delay'] . " seconds.");

                        $job['start'] = time() + $job['respawn_delay'];

                        $this->setJobStatus($id, STATUS_QUEUED);

                        if (array_key_exists($job['service'], $this->services))
                            $this->services[$job['service']]['restarts']++;

                    } else {

                        $this->setJobStatus($id, STATUS_COMPLETE);

                        // Expire the job in 30 seconds
                        $job['expire'] = time();

                    }

                    if (array_key_exists('client', $job)) {

                        $this->unsubscribe($this->clients[$job['client']]);

                        $this->sendAdminEvent('remove', array(
                            'type' => 'client',
                            'client' => $job['client']
                        ));

                        unset($this->clients[$job['client']]);

                        unset($job['client']);

                    }

                } else {

                    if (!array_key_exists('term', $proc) || $proc['term'] == FALSE) {

                        foreach($proc['pipes'] as $sid => $pipe) {

                            if ($output = stream_get_contents($pipe))
                                stdout(W_DEBUG, (($sid >= 1) ? (($sid == 1) ? '> ' : '>> ') : '<< ') . $output, $id);

                            fclose($pipe);

                        }

                        stdout(W_INFO, "Process exited with return code: " . $status['exitcode'], $id);

                    }

                    if ($status['exitcode'] > 0) {

                        stdout(W_WARN, 'Execution completed with error.', $id);

                        if ($job['retries'] >= $this->config->job->retries) {

                            stdout(W_ERR, 'Cancelling job due to too many retries.', $id);

                            $this->setJobStatus($id, STATUS_ERROR);

                            $this->stats['failed']++;

                        } else {

                            stdout(W_DEBUG, 'Re-queuing job for execution.', $id);

                            $this->setJobStatus($id, STATUS_QUEUED_RETRY);

                            $job['start'] = time() + $this->config->job->retry;

                            $job['retries']++;

                        }

                    } else {

                        stdout(W_INFO, 'Execution completed successfully.', $id);

                        $this->stats['execs']++;

                        $this->setJobStatus($id, STATUS_COMPLETE);

                        // Expire the job in 30 seconds
                        $job['expire'] = time() + $this->config->job->expire;

                    }

                }

                proc_close($proc['process']);

            } elseif ($job['status'] == STATUS_CANCELLED) {

                stdout(W_INFO, 'Killing cancelled process');

                if ($job['type'] == 'service') {

                    $this->unsubscribe($this->clients[$job['client']]);

                    $this->sendAdminEvent('remove', array(
                        'type' => 'client',
                        'client' => $job['client']
                    ));

                    unset($this->clients[$job['client']]);

                    unset($job['client']);

                }

                proc_terminate($proc['process']);

                $this->stats['processes']--;

                unset($this->procs[$id]);

                $this->sendAdminEvent('remove', array(
                    'type' => 'process',
                    'id' => $id
                ));

                $this->setJobStatus($id, STATUS_COMPLETE);

                $timeout = ($proc['type'] == 'service') ? 0 : $this->config->job->expire;

                $job['expire'] = time() + $timeout;

            } elseif ($proc['type'] != 'service' && time() >= ($proc['start'] + $this->config->exec->timeout)) {

                stdout(W_WARN, "Process taking too long to execute - Attempting to kill it.", $id);

                if (proc_terminate($proc['process'])) {

                    foreach($proc['pipes'] as $pipe)
                        fclose($pipe);

                    $proc['term'] = TRUE;

                    stdout(W_DEBUG, 'Terminate signal sent.', $id);

                } else {

                    stdout(W_ERR, 'Failed to send terminate signal.', $id);

                }

            } elseif ($status['running'] === TRUE) {

                //DO nothing and let it run!

            }

        }

        return TRUE;

    }

    private function queueCleanup() {

        if (!is_array($this->eventQueue))
            $this->eventQueue = array();

        if (count($this->eventQueue) > 0) {

            foreach($this->eventQueue as $event_id => $events) {

                foreach($events as $id => $data) {

                    if (($data['when'] + $this->config->event->queue_timeout) <= time()) {

                        if ($event_id != $this->config->admin->trigger) {

                            stdout(W_NOTICE, "EXPIRE: NAME=$event_id TRIGGER=$id");

                            $this->sendAdminEvent('remove', array(
                                'type' => 'event',
                                'id' => $id
                            ));

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

            if (!array_key_exists($field, $array))
                return FALSE;

            $array = &$array[$field];

            next($search);

        }

        return TRUE;

    }

    private function getFieldValue($search, $array) {

        reset($search);

        while($field = current($search)) {

            if (!array_key_exists($field, $array))
                return FALSE;

            $array = &$array[$field];

            next($search);

        }

        return $array;

    }

    /**
     * Tests whether a event should be filtered.
     *
     * Returns TRUE if the event should be filtered (skipped), and FALSE if the event should be processed.
     *
     * @param string $event
     *            The event to check.
     *
     * @param Array $filter
     *            The filter rule to test against.
     *
     * @return bool Returns TRUE if the event should be filtered (skipped), and FALSE if the event should be processed.
     */
    private function filterEvent($event, $filter = NULL) {

        if (!($filter && is_array($filter)))
            return FALSE;

        stdout(W_DEBUG, 'Checking event filter for \'' . $event['id'] . '\'');

        foreach($filter as $field => $data) {

            $field = explode('.', $field);

            if ($this->fieldExists($field, $event['data'])) {

                $field_value = $this->getFieldValue($field, $event['data']);

                if (is_array($data)) { // If $data is an array it's a complex filter

                    foreach($data as $filter_type => $filter_value) {

                        switch ($filter_type) {
                            case 'is' :

                                if ($field_value != $filter_value)
                                    return TRUE;

                                break;

                            case 'not' :

                                if ($field_value == $filter_value)
                                    return TRUE;

                                break;

                            case 'like' :

                                if (!preg_match($filter_value, $field_value))
                                    return TRUE;

                                break;

                            case 'in' :

                                if (!in_array($field_value, $filter_value))
                                    return TRUE;

                                break;

                            case 'nin' :

                                if (in_array($field_value, $filter_value))
                                    return TRUE;

                                break;

                        }

                    }

                } else { // Otherwise it's a simple filter with an acceptable value in it

                    if ($field_value != $data)
                        return TRUE;

                }

            }

        }

        return FALSE;

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
    private function processEventQueue(&$client, $event_id, $filter = NULL) {

        stdout(W_DEBUG, "QUEUE: NAME=$event_id");

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
                        stdout(W_NOTICE, "SEEN: NAME=$event_id TRIGGER=$trigger_id CLIENT=$client->id");

                    if (!$client->sendEvent($event['id'], $trigger_id, $event['data']))
                        return FALSE;

                }

            }

        }

        return TRUE;

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
            return FALSE;

        // Get a list of triggers to process
        $triggers = (empty($trigger_id) ? array_keys($this->eventQueue[$event_id]) : array(
            $trigger_id
        ));

        foreach($triggers as $trigger) {

            if (!isset($this->eventQueue[$event_id][$trigger]))
                continue;

            $event = &$this->eventQueue[$event_id][$trigger];

            if (array_key_exists($event_id, $this->waitQueue)) {

                if (!array_key_exists('seen', $event) || !is_array($event['seen']))
                    $event['seen'] = array();

                foreach($this->waitQueue[$event_id] as $item) {

                    if (!array_key_exists($item['client'], $this->clients))
                        continue;

                    $client = $this->clients[$item['client']];

                    if (!in_array($client->id, $event['seen'])) {

                        if ($this->filterEvent($event, $item['filter']))
                            continue;

                        $event['seen'][] = $client->id;

                        if ($event_id != $this->config->admin->trigger)
                            stdout(W_DEBUG, "SEEN: NAME=$event_id TRIGGER=$trigger CLIENT=$client->id");

                        if (!$client->sendEvent($event_id, $trigger, $event['data']))
                            return FALSE;

                    }

                }

            }

        }

        return TRUE;

    }

    private function ping($client_id) {

        $clients = array();

        if ($client_id == 'all') {

            $clients = $this->clients;

        } else {

            if (!array_key_exists($client_id, $this->clients))
                return FALSE;

            $clients[] = array(
                $this->clients[$client_id]
            );

        }

        foreach($clients as $client) {

            $frame = $this->frame('', 'ping', FALSE);

            if (!socket_write($client->resource, $frame, strlen($frame)))
                return FALSE;

        }

        return TRUE;

    }

    private function sendAdminEvent($command, $data = array(), $force_queue = FALSE) {

        $event = $this->config->admin->trigger;

        if ($force_queue === FALSE && !array_key_exists($event, $this->waitQueue))
            return FALSE;

        $trigger_id = uniqid();

        $status = $this->getStatus(FALSE);

        $this->eventQueue[$event][$trigger_id] = array(
            'id' => $event,
            'when' => time(),
            'data' => array(
                'command' => $command,
                'args' => $data,
                'status' => $status
            )
        );

        // Check to see if there are any clients waiting for this event and send notifications to them all.
        $this->processSubscriptionQueue($event, $trigger_id);

        return TRUE;

    }

    private function serviceEnable($name) {

        if (!array_key_exists($name, $this->services))
            return FALSE;

        stdout(W_INFO, 'Enabling service: ' . $name);

        $service = & $this->services[$name];

        $service['job'] = $job_id = $this->getJobId();

        $job = new \Hazaar\Map(array(
            'id' => $job_id,
            'start' => time(),
            'type' => 'service',
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'service' => $name,
            'status' => STATUS_QUEUED,
            'status_text' => 'queued',
            'tag' => $name,
            'enabled' => TRUE,
            'retries' => 0,
            'respawn' => FALSE,
            'respawn_delay' => 5
        ), $service);

        $service['enabled'] = TRUE;

        $service['status'] = 'starting';

        $service['restarts'] = 0;

        $service['last_heartbeat'] = NULL;

        $service['heartbeats'] = 0;

        $this->jobQueue[$job_id] = $job->toArray();

        $this->sendAdminEvent('update', array(
            'type' => 'service',
            'service' => $service
        ));

        $this->stats['queue']++;

        $this->sendAdminEvent('add', array(
            'type' => 'job',
            'id' => $job_id,
            'job' => $this->jobQueue[$job_id]
        ));

        return TRUE;

    }

    private function serviceDisable($name) {

        if (!array_key_exists($name, $this->services))
            return FALSE;

        stdout(W_INFO, 'Disabling service: ' . $name);

        $service = & $this->services[$name];

        $service['enabled'] = FALSE;

        $service['status'] = 'stopping';

        if ($job_id = ake($service, 'job')) {

            $this->setJobStatus($job_id, STATUS_CANCELLED);

            $this->jobQueue[$job_id]['expire'] = time() + $this->config->job->expire;

            if (array_key_exists($job_id, $this->procs)) {

                $out = $this->procs[$job_id]['pipes'][0];

                $this->send($out, 'cancel');

            }

        }

        $this->sendAdminEvent('update', array(
            'type' => 'service',
            'service' => $service
        ));

        return TRUE;

    }

}

if (($we = getenv('WARLOCK_EXEC')) && $we == 1) {

    $warlock = new Server(boolify(getenv('WARLOCK_OUTPUT') == 'file'));

    exit($warlock->bootstrap()->run());

} else {

    die("You can not execute Warlock directly from the command line!\n");

}
