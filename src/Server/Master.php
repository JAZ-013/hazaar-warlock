<?php

namespace Hazaar\Warlock\Server;

class Master extends \Hazaar\Warlock\Protocol\WebSockets {

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
    private $processes = array();
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

        \Hazaar\Warlock\Config::$default_config['sys']['id'] = crc32(APPLICATION_PATH);

        parent::__construct(array(
            'warlock'
        ));

        global $STDOUT;

        global $STDERR;

        Master::$instance = $this;

        $this->silent = $silent;

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, \Hazaar\Warlock\Config::$default_config);

        if(!$this->config->loaded())
            throw new \Exception('There is no warlock configuration file.  Warlock is disabled!');

        if(!$this->config->sys['php_binary'])
            $this->config->sys['php_binary'] = dirname(PHP_BINARY) . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) == 'WIN')?'.exe':'');

        if($tz = $this->config->sys->get('timezone'))
            date_default_timezone_set($tz);

        Logger::set_default_log_level($this->config->log->level);

        $this->log = new Logger();

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

        $this->log->write(W_INFO, 'PHP Version = ' . PHP_VERSION);

        $this->log->write(W_INFO, 'PHP Binary = ' . $this->config->sys['php_binary']);

        $this->log->write(W_INFO, 'Application path = ' . APPLICATION_PATH);

        $this->log->write(W_INFO, 'Application name = ' . APPLICATION_NAME);

        $this->log->write(W_INFO, 'Library path = ' . LIBRAY_PATH);

        $this->log->write(W_INFO, 'Application environment = ' . APPLICATION_ENV);

        $this->log->write(W_INFO, 'PID = ' . $this->pid);

        $this->log->write(W_INFO, 'PID file = ' . $this->pidfile);

        $this->log->write(W_INFO, 'Server ID = ' . $this->config->sys->id);

        $this->log->write(W_NOTICE, 'Listen address = ' . $this->config->server->listen);

        $this->log->write(W_NOTICE, 'Listen port = ' . $this->config->server->port);

        $this->log->write(W_NOTICE, 'Job expiry = ' . $this->config->job->expire . ' seconds');

        $this->log->write(W_NOTICE, 'Exec timeout = ' . $this->config->exec->timeout . ' seconds');

        $this->log->write(W_NOTICE, 'Process limit = ' . $this->config->exec->limit . ' processes');

        $this->protocol = new \Hazaar\Application\Protocol($this->config->sys->id, $this->config->server->encoded);

    }

    public function shutdown($timeout = 0) {

        $this->log->write(W_DEBUG, "SHUTDOWN: TIMEOUT=$timeout");

        $this->shutdown = time() + $timeout;

        return TRUE;

    }

    function __destruct() {

        if (file_exists($this->pidfile))
            unlink($this->pidfile);

        $this->log->write(W_INFO, 'Exiting...');

    }

    /**
     * @brief Returns the application runtime directory
     *
     * @detail The runtime directory is a place where HazaarMVC will keep files that it needs to create during
     * normal operation. For example, socket files for background scheduler communication, cached views,
     * and backend applications.
     *
     * @var string $suffix An optional suffix to tack on the end of the path
     *
     * @since 1.0.0
     *
     * @return string The path to the runtime directory
     */
    public function runtimePath($suffix = NULL, $create_dir = FALSE) {

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
                mkdir($full_path, 0775, TRUE);

        } else {

            $full_path = $path;

        }

        return $full_path;

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

            $this->log->write(W_INFO, "Warlock is already running.");

            $this->log->write(W_INFO, "Please stop the currently running instance first.");

            exit(1);

        }

        $this->log->write(W_NOTICE, 'Creating TCP socket');

        $this->master = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if (!$this->master) {

            $this->log->write(W_ERR, 'Unable to create AF_UNIX socket');

            return 1;

        }

        $this->log->write(W_NOTICE, 'Configuring TCP socket');

        if (!socket_set_option($this->master, SOL_SOCKET, SO_REUSEADDR, 1)) {

            $this->log->write(W_ERR, "Failed: socket_option()");

            return 1;

        }

        $this->log->write(W_NOTICE, 'Binding to socket on ' . $this->config->server->listen . ':' . $this->config->server->port);

        if (!socket_bind($this->master, $this->config->server->listen, $this->config->server->port)) {

            $this->log->write(W_ERR, 'Unable to bind to ' . $this->config->server->listen . ' on port ' . $this->config->server->port);

            return 1;

        }

        if (!socket_listen($this->master)) {

            $this->log->write(W_ERR, 'Unable to listen on ' . $this->config->server->listen . ':' . $this->config->server->port);

            return 1;

        }

        $this->sockets[0] = $this->master;

        $this->running = TRUE;

        $this->log->write(W_INFO, "Ready for connections...");

        $services = new \Hazaar\Application\Config('service', APPLICATION_ENV);

        if ($services->loaded()) {

            $this->log->write(W_INFO, "Checking for enabled services");

            foreach($services as $name => $options) {

                $this->log->write(W_NOTICE, "Found service: $name");

                $this->services[$name] = new Service($options);

                if ($options['enabled'] === TRUE)
                    $this->serviceEnable($name);

            }

        }

        return $this;

    }

    public function run() {

        $this->start = time();

        file_put_contents($this->pidfile, $this->pid);

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

                $this->rrd->setValue('sockets', count($this->sockets));

                $this->rrd->setValue('clients', count($this->clients));

                $this->rrd->setValue('memory', memory_get_usage());

                $count = 0;

                foreach($this->services as $service) {

                    if ($service['enabled'])
                        $count++;

                }

                $this->rrd->setValue('services', $count);

                if ($this->rrd->update())
                    gc_collect_cycles();

                $this->time = $now;

            }

        }

        if (count($this->processes) > 0) {

            $this->log->write(W_NOTICE, 'Terminating running processes');

            foreach($this->processes as $process)
                $process->cancel();

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

        $this->client_lookup = array();

        $this->eventQueue = array();

        $this->waitQueue = array();

        return 0;

    }

    public function disconnect($socket) {

        $socket_id = intval($socket);

        if ($client = $this->getClient($socket)) {

            $this->log->write(W_DEBUG, 'DISCONNECT: CLIENT=' . $client->id . ' SOCKET=' . $socket . ' COUNT=' . $client->socketCount);

            $this->unsubscribe($client, NULL, $socket);

        }

        $this->removeClient($socket);

        /**
         * Remove the socket from our list of sockets
         */
        if (array_key_exists($socket_id, $this->sockets))
            unset($this->sockets[$socket_id]);

        $this->log->write(W_DEBUG, "SOCKET_CLOSE: SOCKET=" . $socket);

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

            $client = new Socket\Client($this, $id, 'client', $socket, $uid, $this->config->client);

            // Add it to the client array
            $this->clients[$id] = $client;

            $this->stats['clients']++;

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

        $this->log->write(W_DEBUG, "LOOKUP_UNSET: CLIENT=$client->id SOCKETS=" . $client->socketCount);

        if (count($client->subscriptions) <= 0 && $client->socketCount <= 0) {

            $this->log->write(W_DEBUG, "REMOVE: CLIENT=$id");

            unset($this->clients[$id]);

            $this->stats['clients']--;

            return TRUE;

        } else {

            $this->log->write(W_DEBUG, "NOTREMOVE: SUBS=" . count($client->subscriptions) . ' SOCKETS=' . $client->socketCount);

        }

        return FALSE;

    }

    private function getClient($socket) {

        $socket_id = intval($socket);

        return (array_key_exists($socket_id, $this->client_lookup) ? $this->clients[$this->client_lookup[$socket_id]] : NULL);

    }

    private function processClient($socket) {

        @$bytes_received = socket_recv($socket, $buf, 65536, 0);

        if ($bytes_received == 0) {

            $this->log->write(W_NOTICE, 'Remote host closed connection');

            $this->disconnect($socket);

            return FALSE;

        }

        @$status = socket_getpeername($socket, $address, $port);

        if ($status == FALSE) {

            $this->disconnect($socket);

            return FALSE;

        }

        $this->log->write(W_DEBUG, "SOCKET_RECV: HOST=$address:$port BYTES=" . strlen($buf));

        $client = $this->getClient($socket);

        if (!$client instanceof Socket\Client) {

            $result = $this->initiateHandshake($socket, $buf);

            if (!$result)
                $this->disconnect($socket);

        } else {

            //Record this time as the last time we received data from the client
            $client->lastContact = time();

            /**
             * Sometimes we can get multiple frames in a single buffer so we cycle through them until they are all processed.
             * This will even allow partial frames to be added to the client frame buffer.
             */
            while($frame = $this->processFrame($buf, $client)) {

                $this->log->write(W_DECODE, "RECV_PACKET: " . $frame);

                $payload = null;

                $time = null;

                $type = $this->protocol->decode($frame, $payload, $time);

                if ($type) {

                    $client->offset = (time() - $time);

                    if (!$this->processCommand($socket, $client, $type, $payload, $time)) {

                        $this->log->write(W_ERR, 'An error occurred processing the command TYPE: ' . $type);

                        $this->send($socket, 'error', array(
                            'reason' => 'An error occurred processing the command',
                            'command' => $type
                        ));

                    }

                } else {

                    $reason = $this->protocol->getLastError();

                    $this->log->write(W_ERR, "Protocol error: $reason");

                    $this->send($socket, $this->protocol->encode('error', array(
                        'reason' => $reason
                    )));

                }

            }

        }

        return TRUE;

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

            $this->log->write(W_WARN, 'Unable to parse request while initiating WebSocket handshake!');

            return false;

        }

        $url = NULL;

        $responseCode = FALSE;

        if (array_key_exists('connection', $headers) && preg_match('/upgrade/', strtolower($headers['connection']))) {

            if (array_key_exists('get', $headers) && ($responseCode = $this->acceptHandshake($headers, $responseHeaders, NULL, $results)) == 101) {

                $this->log->write(W_NOTICE, "Initiating WebSockets handshake");

                if (!($cid = $results['url']['CID']))
                    return FALSE;

                $client = $this->addClient($cid, $socket, (array_key_exists('UID', $results['url']) ? $results['url']['UID'] : NULL));

                $client->socketCount++;

                $response = $this->httpResponse($responseCode, NULL, $responseHeaders);

                $result = @socket_write($socket, $response, strlen($response));

                if($result !== false && $result > 0){

                    $this->log->write(W_NOTICE, 'WebSockets handshake successful!');

                    return TRUE;

                }

            } elseif ($responseCode > 0) {

                $responseHeaders['Connection'] = 'close';

                $responseHeaders['Content-Type'] = 'text/text';

                $body = $responseCode . ' ' . http_response_text($responseCode);

                $response = $this->httpResponse($responseCode, $body, $responseHeaders);

                $this->log->write(W_WARN, "Handshake failed with code $responseCode");

                @socket_write($socket, $response, strlen($response));

            }

        } else {

            /**
             * Legacy long polling handshake
             */

            $this->log->write(W_NOTICE, "Processing legacy request");

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

                $this->log->write(W_DEBUG, "FOUND: CLIENT=$client->id SOCKET=$socket COUNT=$client->socketCount");

            } else {

                $client = new Socket\Client($this, $query['CID'], 'client', null, (array_key_exists('UID', $query) ? $query['UID'] : null));

                $client->socketCount = 1;

                socket_getpeername($socket, $client->address, $client->port);

                $this->clients[$query['CID']] = $client;

                $this->log->write(W_DEBUG, "ADD: CLIENT=$client->id MODE=legacy SOCKET=$socket COUNT=$client->socketCount");

            }

            $payload = null;

            $socket_id = intval($socket);

            $this->client_lookup[$socket_id] = $client->id;

            $responseHeaders['Connection'] = 'Keep-alive';

            $responseHeaders['Access-Control-Allow-Origin'] = $headers['origin'];

            $responseHeaders['Content-Type'] = 'text/text';

            $type = $this->protocol->decode($query['P'], $payload);

            if ($type) {

                $response = $this->httpResponse(200, NULL, $responseHeaders);

                @socket_write($socket, $response, strlen($response));

                $client->handshake = TRUE;

                $result = $this->processCommand($socket, $client, $type, $payload, time());

                return !$result; // If $result is TRUE then we disconnect. If it is FALSE we do NOT disconnect.

            } else {

                $reason = $this->protocol->getLastError();

                $this->log->write(W_ERR, "Connection rejected - $reason");

                $response = $this->httpResponse(200, $this->protocol->encode('error', array(
                    'reason' => $reason
                )), $responseHeaders);

                @socket_write($socket, $response, strlen($response));

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

    private function processFrame(&$frameBuffer, Socket\Client &$client) {

        if ($client->frameBuffer) {

            $frameBuffer = $client->frameBuffer . $frameBuffer;

            $client->frameBuffer = NULL;

            return $this->processFrame($frameBuffer, $client);

        }

        if (!$frameBuffer)
            return FALSE;

        $this->log->write(W_DECODE2, "RECV_FRAME: " . implode(' ', $this->hexString($frameBuffer)));

        $opcode = $this->getFrame($frameBuffer, $payload);

        /**
         * If we get an opcode that equals FALSE then we got a bad frame.
         *
         * If we get a opcode of -1 there are more frames to come for this payload. So, we return FALSE if there are no
         * more frames to process, or TRUE if there are already more frames in the buffer to process.
         */
        if ($opcode === FALSE) {

            $this->log->write(W_ERR, 'Bad frame received from client. Disconnecting.');

            $this->disconnect($client->socket);

            return FALSE;

        } elseif ($opcode === -1) {

            $client->payloadBuffer .= $payload;

            return (strlen($frameBuffer) > 0);

        }

        $this->log->write(W_DECODE2, "OPCODE: $opcode");

        //Save any leftover frame data in the client framebuffer
        if (strlen($frameBuffer) > 0) {

            $client->frameBuffer = $frameBuffer;

            $frameBuffer = '';

        }

        //If we have data in the payload buffer (because we previously received OPCODE -1) then retrieve it here.
        if ($client->payloadBuffer) {

            $payload = $client->payloadBuffer . $payload;

            $client->payloadBuffer = '';

        }

        //Check the WebSocket OPCODE and see if we need to do any internal processing like PING/PONG, CLOSE, etc.
        switch ($opcode) {

            case 0 :
            case 1 :
            case 2 :

                break;

            case 8 :

                if($client->closing === false){

                    $this->log->write(W_DEBUG, "WEBSOCKET_CLOSE: HOST=$client->address:$client->port");

                    $client->closing = TRUE;

                    $frame = $this->frame('', 'close', FALSE);

                    @socket_write($client->resource, $frame, strlen($frame));

                    if(($count = count($client->jobs)) > 0){

                        $this->log->write(W_NOTICE, 'Disconnected WebSocket client has ' . $count . ' running/pending child jobs');

                        foreach($client->jobs as $id => $job){

                            if($job->detach !== true)
                                $job->status = STATUS_CANCELLED;

                        }

                    }

                    $this->disconnect($client->resource);

                }

                return FALSE;

            case 9 :

                $this->log->write(W_DEBUG, "WEBSOCKET_PING: HOST=$client->address:$client->port");

                $frame = $this->frame('', 'pong', FALSE);

                @socket_write($client->resource, $frame, strlen($frame));

                return FALSE;

            case 10 :

                $this->log->write(W_DEBUG, "WEBSOCKET_PONG: HOST=$client->address:$client->port");

                $client->pong($payload);

                return FALSE;

            default :

                $this->log->write(W_DEBUG, "DISCONNECT: REASON=unknown opcode HOST=$client->address:$client->port");

                $this->disconnect($client->resource);

                return FALSE;

        }

        return $payload;

    }

    public function send($resource, $command, $payload = NULL, $is_legacy = false) {

        if (!is_resource($resource))
            return FALSE;

        if (!is_string($command))
            return FALSE;

        $packet = $this->protocol->encode($command, $payload); //Override the timestamp.

        $this->log->write(W_DECODE, "SEND_PACKET: $packet");

        if ($is_legacy) {

            $frame = $packet;

        } else {

            $frame = $this->frame($packet, 'text', FALSE);

            $this->log->write(W_DECODE2, "SEND_FRAME: " . implode(' ', $this->hexString($frame)));

        }

        return $this->write($resource, $frame, $is_legacy);

    }

    private function write($resource, $frame, $is_legacy = false){

        $len = strlen($frame);

        $this->log->write(W_DEBUG, "SOCKET_WRITE: BYTES=$len SOCKET=$resource LEGACY=" . ($is_legacy ? 'TRUE' : 'FALSE'));

        $bytes_sent = @socket_write($resource, $frame, $len);

        if ($bytes_sent === false) {

            $this->log->write(W_WARN, 'An error occured while sending to the client. Could be disconnected.');

            return FALSE;

        } elseif ($bytes_sent != $len) {

            $this->log->write(W_ERR, $bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected');

            return FALSE;

        }

        return TRUE;

    }

    public function ping($resource) {

        $this->log->write(W_DEBUG, 'PING: RESOURCE=' . $resource);

        $frame = $this->frame('', 'ping', FALSE);

        $this->log->write(W_DEBUG, "SEND_FRAME: " . implode(' ', $this->hexString($frame)));

        return $this->write($resource, $frame, false);

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

        return $status;

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

    private function processCommand($resource, $client, $command, &$payload, $time) {

        if (!$command)
            return FALSE;

        $type = $client->isLegacy() ? 'Legacy' : 'WebSocket';

        $this->log->write(W_DEBUG, "COMMAND: $command" . ($client->id ? " CLIENT=$client->id" : NULL) . " TYPE=$type");

        switch ($command) {

            case 'NOOP':

                $this->log->write(W_INFO, 'NOOP: ' . print_r($payload, true));

                return true;

            case 'SYNC':

                return $this->commandSync($resource, $client, $payload);

            case 'OK':

                if($payload)
                    $this->log->write(W_INFO, $payload);

                return true;

            case 'ERROR':

                $this->log->write(W_ERR, $payload);

                return true;

            case 'STATUS' :

                return $this->commandStatus($resource, $client, $payload);

            case 'SHUTDOWN' :

                return $this->commandStop($resource, $client);

            case 'DELAY' :

                return $this->commandDelay($resource, $client, $payload);

            case 'SCHEDULE' :

                return $this->commandSchedule($resource, $client, $payload);

            case 'CANCEL' :

                return $this->commandCancel($resource, $client, $payload);

            case 'ENABLE' :

                return $this->commandEnable($resource, $client, $payload);

            case 'DISABLE' :

                return $this->commandDisable($resource, $client, $payload);

            case 'SERVICE' :

                return $this->commandService($resource, $client, $payload);

            case 'SUBSCRIBE' :

                $filter = (property_exists($payload, 'filter') ? $payload->filter : NULL);

                return $this->commandSubscribe($resource, $client, $payload->id, $filter);

            case 'UNSUBSCRIBE' :

                return $this->commandUnsubscribe($resource, $client, $payload->id);

            case 'TRIGGER' :

                $data = ake($payload, 'data');

                $echo = ake($payload, 'echo', false);

                return $this->commandTrigger($resource, $client, $payload->id, $data, $echo);

            case 'PING' :

                return $this->send($resource, 'pong', $payload);

            case 'PONG':

                if(is_int($payload)){

                    $trip_ms = (microtime(true) - $payload) * 1000;

                    $this->log->write(W_INFO, 'PONG received in ' . $trip_ms . 'ms');

                }else{

                    $this->log->write(W_WARN, 'PONG received with invalid payload!');

                }

                break;

            case 'LOG':

                return $this->commandLog($resource, $client, $payload);

            case 'SPAWN':

                return $this->commandSpawn($resource, $client, $payload);

            case 'KILL':

                return $this->commandKill($resource, $client, $payload);

            case 'SIGNAL':

                return $this->commandSignal($resource, $client, $payload);

            case 'DEBUG':

                $this->log->write(W_DEBUG, ake($payload, 'data'));

                return true;

        }

        return FALSE;

    }

    private function commandSync($resource, $client, $payload){

        $this->log->write(W_DEBUG, "SYNC: CLIENT_ID=$client->id OFFSET=$client->offset");

        if ($payload instanceof \stdClass
            && property_exists($payload, 'admin_key')
            && $payload->admin_key === $this->config->admin->key) {

            $this->log->write(W_NOTICE, 'Warlock control authorised to ' . $client->id);

            $client->type = 'admin';

            $this->send($resource, 'OK', NULL, $client->isLegacy());

        }elseif($payload instanceof \stdClass
            && property_exists($payload, 'job_id')
            && $payload->client_id === $client->id
            && array_key_exists($payload->job_id, $this->jobQueue)){

            $job =& $this->jobQueue[$payload->job_id];

            if($job->access_key !== ake($payload, 'access_key')){

                $this->log->write(W_ERR, 'Service tried to sync with bad access key!', $payload->job_id);

                return false;

            }

            $job->client = $client;

            $client->type = $job->type;

            $client->jobs[$job->id] = $job;

            $this->log->write(W_NOTICE, ucfirst($client->type) . ' registered successfully', $payload->job_id);

            $this->send($resource, 'OK', NULL, $client->isLegacy());

        } else {

            $this->log->write(W_ERR, 'Client requested bad sync!');

            $client->closing = TRUE;

            return false;

        }

        return TRUE;

    }

    private function commandStop($resource, $client) {

        if ($client->type !== 'admin')
            return FALSE;

        $this->log->write(W_NOTICE, "Shutdown requested");

        $this->send($resource, 'OK', NULL, $client->isLegacy());

        return $this->shutdown(1);

    }

    private function commandStatus($resource, $client, $payload = null) {

        if($payload){

            if($client->type !== 'service'){

                $this->log->write(W_WARN, 'Client sent status but client is not a service!', $client->address);

                return false;

            }

            $job = ake($client->jobs, $payload->job_id);

            if(!$job){

                $this->log->write(W_WARN, 'Service status received for client with no job ID');

                return false;

            }

            $service = ake($job, 'name');

            if(!array_key_exists($service, $this->services)){

                $this->log->write(W_ERR, 'Could not find job for service client!', $client->process['id']);

                return false;

            }

            $this->services[$service]['info'] = $payload;

            $this->services[$service]['last_heartbeat'] = time();

            return true;

        }

        if ($client->type !== 'admin')
            return FALSE;

        return $this->send($resource, 'STATUS', $this->getStatus(), $client->isLegacy());

    }

    private function commandDelay($resource, $client, $command) {

        if ($client->type !== 'admin')
            return FALSE;

        if ($command->value == NULL)
            $command->value = 0;

        $when = time() + $command->value;

        $tag = NULL;

        $tag_overwrite = FALSE;

        if (property_exists($command, 'tag')) {

            $tag = $command->tag;

            $tag_overwrite = $command->overwrite;

        }

        if ($id = $this->scheduleJob($when, $command->function, $command->application, $tag, $tag_overwrite)) {

            $this->log->write(W_NOTICE, "Successfully scheduled delayed function.  Executing in {$command->value} seconds . ", $id);

            return $this->send($resource, 'OK', array('job_id' => $id), $client->isLegacy());

        }

        $this->log->write(W_ERR, "Could not schedule delayed function");

        return $this->send($resource, 'ERROR', NULL, $client->isLegacy());

    }

    private function commandSchedule($resource, $client, $command) {

        if ($client->type !== 'admin')
            return FALSE;

        $tag = NULL;

        $tag_overwrite = FALSE;

        if (property_exists($command, 'tag')) {

            $tag = $command->tag;

            $tag_overwrite = $command->overwrite;

        }

        if ($id = $this->scheduleJob($command->when, $command->function, $command->application, $tag, $tag_overwrite)) {

            $this->log->write(W_NOTICE, "Function execution successfully scheduled", $id);

            return $this->send($resource, 'OK', array('job_id' => $id), $client->isLegacy());

        }

        $this->log->write(W_ERR, "Could not schedule function");

        return $this->send($resource, 'ERROR', NULL, $client->isLegacy());

    }

    private function commandCancel($resource, $client, $job_id) {

        if ($client->type !== 'admin')
            return FALSE;

        if ($this->cancelJob($job_id)) {

            $this->log->write(W_NOTICE, "Job successfully canceled");

            return $this->send($resource, 'ok', NULL, $client->isLegacy());

        }

        $this->log->write(W_ERR, 'Error trying to cancel job');

        return $this->send($resource, 'ERROR', NULL, $client->isLegacy());

    }

    private function commandSubscribe($resource, $client, $event_id, $filter = NULL) {

        if (!$this->subscribe($resource, $client, $event_id, $filter))
            return FALSE;

        //If this is not a legacy client, send the OK immediately
        if (!$client->isLegacy())
            $this->send($resource, 'OK', NULL, $client->isLegacy());

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

        return TRUE;

    }

    private function commandUnsubscribe($resource, $client, $event_id) {

        if ($this->unSubscribe($client, $event_id))
            return $this->send($resource, 'OK', NULL, $client->isLegacy());

        return $this->send($resource, 'ERROR', NULL, $client->isLegacy());

    }

    private function commandTrigger($resource, $client, $event_id, $data, $echo_client = true) {

        $this->log->write(W_NOTICE, "TRIGGER: NAME=$event_id CLIENT=$client->id ECHO=" . strbool($echo_client));

        $this->stats['events']++;

        $this->rrd->setValue('events', 1);

        $trigger_id = uniqid();

        $this->eventQueue[$event_id][$trigger_id] = $new = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'when' => time(),
            'data' => $data,
            'seen' => ($echo_client ? array() : array($client->id))
        );

        if($client instanceof Server\Client)
            $this->send($resource, 'OK', NULL, $client->isLegacy());

        // Check to see if there are any clients waiting for this event and send notifications to them all.
        $this->processSubscriptionQueue($event_id, $trigger_id);

        return TRUE;

    }

    private function commandEnable($resource, $client, $name) {

        if ($client->type !== 'admin')
            return FALSE;

        $this->log->write(W_NOTICE, "ENABLE: NAME=$name CLIENT=$client->id");

        $result = $this->serviceEnable($name);

        $this->send($resource, ($result ? 'OK' : 'ERROR'), NULL, $client->isLegacy());

        return TRUE;

    }

    private function commandDisable($resource, $client, $name) {

        if ($client->type !== 'admin')
            return FALSE;

        $this->log->write(W_NOTICE, "DISABLE: NAME=$name CLIENT=$client->id");

        $result = $this->serviceDisable($name);

        $this->send($resource, ($result ? 'OK' : 'ERROR'), NULL, $client->isLegacy());

        return TRUE;

    }

    private function commandService($resource, $client, $name) {

        if ($client->type !== 'admin')
            return FALSE;

        $this->log->write(W_NOTICE, "SERVICE: NAME=$name CLIENT=$client->id");

        if(array_key_exists($name, $this->services)){

            $this->send($resource, 'SERVICE', $this->services[$name], $client->isLegacy());

            return true;

        }

        $this->send($resource, 'ERROR', NULL, $client->isLegacy());

        return false;

    }

    private function commandLog($resource, $client, $payload){

        if(!array_key_exists('msg', $payload))
            return false;

        $level = ake($payload, 'level', W_INFO);

        if(is_array($payload->msg)){

            foreach($payload->msg as $msg){

                if(!$this->commandLog($resource, $client, (object)array('level' => $level, 'msg' => $msg)))
                    return false;

            }

        }else{

            $this->log->write($level, ake($payload, 'msg', '--'));

        }

        return true;

    }

    private function commandSpawn($resource, $client, $payload){

        if(!($name = ake($payload, 'name')))
            return false;

        if (!array_key_exists($name, $this->services))
            return FALSE;

        $job_id = $this->getJobId();

        $this->log->write(W_NOTICE, 'Spawning dynamic service: ' . $name, $job_id);

        $service = & $this->services[$name];

        $this->jobQueue[$job_id] = new Job\Service(array(
            'id' => $job_id,
            'name' => $name,
            'start' => time(),
            'type' => 'service',
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'status' => STATUS_QUEUED,
            'status_text' => 'queued',
            'tag' => $name,
            'enabled' => TRUE,
            'dynamic' => TRUE,
            'detach' => ake($payload, 'detach', false),
            'retries' => 0,
            'respawn' => FALSE,
            'respawn_delay' => 5,
            'parent' => $client,
            'params' => ake($payload, 'params')
        ), $service);

        $client->jobs[$job_id] =& $this->jobQueue[$job_id];

        $this->stats['queue']++;

        return TRUE;

    }

    private function commandKill($resource, $client, $payload){

        if(!($name = ake($payload, 'name')))
            return false;

        if (!array_key_exists($name, $this->services))
            return FALSE;

        foreach($client->jobs as $id => $job){

            $this->log->write(W_NOTICE, "KILL: SERVICE=$name JOB_ID=$id CLIENT={$client->id}");

            $client->jobs[$id]->status = STATUS_CANCELLED;

            unset($client->jobs[$id]);

        }

        return true;

    }

    private function commandSignal($resource, $client, $payload){

        if(!($event_id = ake($payload, 'id')))
            return false;

        //Otherwise, send this signal to any child services for the requested type
        if(!($service = ake($payload, 'service')))
            return false;

        $trigger_id = uniqid();

        $data = ake($payload, 'data');

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(TRUE),
            'data' => $data
        );

        //If this is a message coming from the service, send it back to it's parent client connection
        if($client->type == 'service'){

            $this->log->write(W_NOTICE, "SIGNAL: SERVICE=$service JOB_ID={$client->process['id']} CLIENT={$client->id}");

            $resource = $client->process['parent']->resource;

            $result = $this->send($resource, 'EVENT', $packet, $client->process['parent']->isLegacy());

        }else{

            foreach($client->jobs as $id => $job){

                if(!array_key_exists($id, $this->jobQueue))
                    continue;

                $this->log->write(W_NOTICE, "SIGNAL: SERVICE=$service JOB_ID=$id CLIENT={$client->id}");

                $resource = $job->client->resource;

                $result = $this->send($resource, 'EVENT', $packet, $job->client->isLegacy());

                // Disconnect if we are a socket but not a websocket (legacy connection) and the result was negative.
                if (get_resource_type($resource) == 'Socket' && $job->client->isLegacy() && $result)
                    $this->disconnect($resource);

            }

        }

        return true;

    }

    public function subscribe($resource, $client, $event_id, $filter) {

        if ($client->isSubscribed($event_id)){

            $this->log->write(W_WARN, 'Client is already subscribed to event: ' . $event_id);

            return true;

        }

        $new = array(
            'client' => $client->id,
            'since' => time(),
            'filter' => $filter
        );

        $this->waitQueue[$event_id][] = $new;

        $client->subscribe($event_id, $resource);

        if ($event_id == $this->config->admin->trigger) {

            $this->log->write(W_DEBUG, "ADMIN_SUBSCRIBE: CLIENT=$client->id");

        } else {

            $this->stats['subscriptions']++;

        }

        return TRUE;

    }

    public function unsubscribe($client, $event_id = NULL, $resource = NULL) {

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

                            $this->log->write(W_DEBUG, "DEQUEUE: NAME=$event_id CLIENT=$client->id");

                            unset($this->waitQueue[$event_id][$id]);

                            if ($event_id == $this->config->admin->trigger) {

                                $this->log->write(W_DEBUG, "ADMIN_UNSUBSCRIBE: CLIENT=$client->id");

                            } else {

                                $this->stats['subscriptions']--;

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

                $this->log->write(W_ERR, "Unable to generate job ID after $count attempts . Giving up . This is bad! ");

                return FALSE;

            }

        }

        return $jid;

    }

    private function scheduleJob($when, $function, $application, $tag = NULL, $overwrite = FALSE) {

        if(!property_exists($function, 'code')){

            $this->log->write(W_ERR, 'Unable to schedule job without function code!');

            return false;

        }

        if (!($id = $this->getJobId()))
            return FALSE;

        $this->log->write(W_DEBUG, "JOB: ID=$id");

        if (!is_numeric($when)) {

            $this->log->write(W_DEBUG, 'Parsing string time', $id);

            $when = strtotime($when);

        }

        $this->log->write(W_NOTICE, 'NOW:  ' . date('c'), $id);

        $this->log->write(W_NOTICE, 'WHEN: ' . date('c', $when), $id);

        $this->log->write(W_NOTICE, 'APPLICATION_PATH: ' . $application->path, $id);

        $this->log->write(W_NOTICE, 'APPLICATION_ENV:  ' . $application->env, $id);

        if (!$when || $when < time()) {

            $this->log->write(W_WARN, 'Trying to schedule job to execute in the past', $id);

            return FALSE;

        }

        $this->log->write(W_NOTICE, 'Scheduling job for execution at ' . date('c', $when), $id);

        if ($tag) {

            $this->log->write(W_NOTICE, 'TAG: ' . $tag, $id);

            if (array_key_exists($tag, $this->tags)) {

                $this->log->write(W_NOTICE, "Job already scheduled with tag $tag", $id);

                if ($overwrite == 'true') {

                    $id = $this->tags[$tag];

                    $this->log->write(W_NOTICE, 'Overwriting', $id);

                } else {

                    $this->log->write(W_NOTICE, 'Skipping', $id);

                    return FALSE;

                }

            }

            $this->tags[$tag] = $id;

        }

        $this->jobQueue[$id] = new Job\Runner(array(
            'id' => $id,
            'start' => $when,
            'type' => 'job',
            'application' => array(
                'path' => $application->path,
                'env' => $application->env
            ),
            'function' => $function->code,
            'params' => ake($function, 'params', array()),
            'tag' => $tag,
            'status' => STATUS_QUEUED,
            'retries' => 0,
            'expire' => 0
        ));

        $this->log->write(W_NOTICE, 'Job added to queue', $id);

        $this->stats['queue']++;

        return $id;

    }

    private function cancelJob($job_id) {

        $this->log->write(W_DEBUG, 'Trying to cancel job', $job_id);

        //If the job IS is not found return false
        if (!array_key_exists($job_id, $this->jobQueue))
            return FALSE;

        if (array_key_exists('tag', $this->jobQueue[$job_id]))
            unset($this->tags[$this->jobQueue[$job_id]['tag']]);

        $job =& $this->jobQueue[$job_id];

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

        return TRUE;

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

                if ($job instanceof Job\Runner){

                    if ($job->retries > 0)
                        $this->stats['retries']++;

                    $this->rrd->setValue('jobs', 1);

                    $this->log->write(W_INFO, "Starting job execution", $id);

                    $this->log->write(W_NOTICE, 'NOW:  ' . date('c', $now), $id);

                    $this->log->write(W_NOTICE, 'WHEN: ' . date('c', $job->start), $id);

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

                    $job->process = $process;

                    $job->status = STATUS_RUNNING;

                    $this->stats['processes']++;

                    $payload = array(
                        'application_name' => APPLICATION_NAME,
                        'server_port' => $this->config->server['port'] ,
                        'job_id' => $id,
                        'access_key' => $job->access_key,
                        'timezone' => date_default_timezone_get(),
                        'config' => array('app' => array('root' => '/')) //Sets the default web root to / but this can be overridden in service config
                    );

                    $packet = null;

                    if ($job instanceof Job\Service) {

                        $payload['name'] = $job->name;

                        if($config = $job->config)
                            $payload['config'] = array_merge($payload['config'], $config);

                        $packet = $this->protocol->encode('service', $payload);

                    } elseif ($job instanceof Job\Runner) {

                        $payload['function'] = $job->function;

                        if ($job->has('params') && is_array($job->params) && count($job->params) > 0)
                            $payload['params'] = $job->params;

                        $packet = $this->protocol->encode('exec', $payload);

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

                $status = $job->process->status;

                if ($status['running'] === FALSE) {

                    $this->stats['processes']--;

                    $job->process->close();

                    /**
                     * Process a Service shutdown.
                     */
                    if ($job instanceof Job\Service) {

                        $name = $job->name;

                        $this->services[$name]['status'] = 'stopped';

                        $this->log->write(W_DEBUG, "SERVICE=$name EXIT=$status[exitcode]");

                        if ($status['exitcode'] > 0 && $job->status !== STATUS_CANCELLED) {

                            $this->log->write(W_ERR, "Service '$name' returned status code $status[exitcode]");

                            if ($status['exitcode'] == 2) {

                                $this->log->write(W_ERR, 'Service failed to start because service class does not exist.  Disabling service.');

                                $job->status = STATUS_ERROR;

                                continue;

                            } elseif ($status['exitcode'] == 3) {

                                $this->log->write(W_ERR, 'Service failed to start because it has missing required modules.  Disabling service.');

                                $job->status = STATUS_ERROR;

                                continue;

                            } elseif ($status['exitcode'] == 4) {

                                $this->log->write(W_ERR, 'Service exited because it lost the control channel.  Restarting.');

                            } elseif ($status['exitcode'] == 5) {

                                $this->log->write(W_ERR, 'Dynamic service failed to start because it has no runOnce() method!');

                                $job->status = STATUS_ERROR;

                                continue;

                            }

                            $job->retries++;

                            $job->status = STATUS_QUEUED_RETRY;

                            if ($job->retries > $this->config->service->restarts) {

                                if($job->dynamic === true){

                                    $this->log->write(W_WARN, "Dynamic service '$name' is restarting too often.  Cancelling spawn.");

                                    $this->cancelJob($job->id);

                                }else{

                                    $this->log->write(W_WARN, "Service '$name' is restarting too often.  Disabling for {$this->config->service->disable} seconds.");

                                    $job->start = time() + $this->config->service->disable;

                                    $job->retries = 0;

                                    $job->expire = 0;

                                }

                            } else {

                                $this->log->write(W_NOTICE, "Restarting service '$name'. ({$job->retries})");

                                if (array_key_exists($job->service, $this->services))
                                    $this->services[$job->service]['restarts']++;

                            }

                        } elseif ($job->respawn == TRUE && $job->status == STATUS_RUNNING) {

                            $this->log->write(W_NOTICE, "Respawning service '$name' in " . $job->respawn_delay . " seconds.");

                            $job->start = time() + $job->respawn_delay;

                            $job->status = STATUS_QUEUED;

                            if (array_key_exists($job->service, $this->services))
                                $this->services[$job->service]['restarts']++;

                        } else {

                            $job->status = STATUS_COMPLETE;

                            // Expire the job in 30 seconds
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

                            $this->log->write(W_INFO, 'Execution completed successfully.', $id);

                            $this->stats['execs']++;

                            $job->status = STATUS_COMPLETE;

                            // Expire the job in 30 seconds
                            $job->expire = time() + $this->config->job->expire;

                        }

                    }

                    $job->process = null;

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

        $this->log->write(W_DEBUG, 'Checking event filter for \'' . $event['id'] . '\'');

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
     * @param Socket\Client $client
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

        $this->log->write(W_DEBUG, "EVENT_QUEUE: NAME=$event_id COUNT=" . count($this->eventQueue[$event_id]));

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
                            $this->log->write(W_DEBUG, "SEEN: NAME=$event_id TRIGGER=$trigger CLIENT=$client->id");

                        if (!$client->sendEvent($event_id, $trigger, $event['data']))
                            return FALSE;

                    }

                }

            }

        }

        return TRUE;

    }

    private function serviceEnable($name, $options = null) {

        if (!array_key_exists($name, $this->services))
            return FALSE;

        $this->log->write(W_INFO, 'Enabling service: ' . $name);

        $service =& $this->services[$name];

        $service['job'] = $job_id = $this->getJobId();

        $this->jobQueue[$job_id] = new Job\Service(array(
            'id' => $job_id,
            'name' => $name,
            'start' => time(),
            'type' => 'service',
            'enabled' => true,
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'status' => STATUS_QUEUED,
            'status_text' => 'queued',
            'tag' => $name,
            'retries' => 0,
            'respawn' => false,
            'respawn_delay' => 5
        ), $service);

        $this->stats['queue']++;

        return TRUE;

    }

    private function serviceDisable($name) {

        if (!array_key_exists($name, $this->services))
            return FALSE;

        $service = &$this->services[$name];

        if(!$service['enabled'])
            return false;

        $this->log->write(W_INFO, 'Disabling service: ' . $name);

        $service['enabled'] = false;

        if ($job_id = ake($service, 'job')) {

            $job =& $this->jobQueue($job_id);

            $job->status = STATUS_CANCELLED;

            $job->expire = time() + $this->config->job->expire;

            if ($job->process)
                $this->send($job->client, 'cancel');

        }

        return TRUE;

    }

}
