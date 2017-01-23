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
class Control extends WebSockets {

    public  $config;

    public  $protocol;

    private $id;

    private $cmd;

    private $outputfile;

    private $pidfile;

    private $server_pid;

    private $socket;

    private $closing = FALSE;

    private $key;

    function __construct($autostart = NULL) {

        if(! extension_loaded('sockets'))
            throw new \Exception('The sockets extension is not loaded.');

        parent::__construct('warlock');

        $cache = new \Hazaar\Cache('session');

        if(($this->id = $cache->get('warlock_guid')) == FALSE) {

            $this->id = guid();

            $cache->set('warlock_guid', $this->id);

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

        $this->config = new \Hazaar\Application\Config('warlock', NULL, $defaults);

        $app = \Hazaar\Application::getInstance();

        $this->outputfile = $app->runtimePath($this->config->log->file);

        $this->pidfile = $app->runtimePath($this->config->sys->pid);

        $this->key = uniqid();

        $this->protocol = new \Hazaar\Application\Protocol($this->config->sys->id, $this->config->server->encoded);

        /**
         * First we check to see if we need to start the Warlock server process
         */
        if($autostart === NULL)
            $autostart = (boolean)$this->config->sys->autostart;

        if($this->isRunning()) {

            if(! $this->connect()) {

                $this->disconnect(FALSE);

                throw new \Exception('Warlock is already running but we were unable to communicate with it.');

            }

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

    function __destruct() {

        $this->disconnect(FALSE);

    }

    private function connect() {

        $this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        if(! $this->socket)
            die('Unable to create AF_UNIX socket');

        socket_connect($this->socket, $this->config->server->listen, $this->config->server->port);

        $url = NULL;

        if(! array_key_exists('PWD', $_SERVER))
            $url = new \Hazaar\Application\Url();

        /**
         * Initiate a WebSockets connection
         */
        $handshake = $this->createHandshake('/' . APPLICATION_NAME . '/warlock?CID=' . $this->id, $this->config->server->listen, ($url ? $url->renderObject(FALSE) : NULL), $this->key);

        socket_write($this->socket, $handshake, strlen($handshake));

        /**
         * Wait for the response header
         */
        $read = array($this->socket);

        $write = $except = NULL;

        $sockets = socket_select($read, $write, $except, 3000);

        if($sockets == 0) return FALSE;

        socket_recv($this->socket, $buf, 65536, 0);

        $response = $this->parseHeaders($buf);

        if($response['code'] != 101)
            throw new \Exception('Walock server returned status: ' . $response['code'] . ' ' . $response['status']);

        if(! $this->acceptHandshake($response, $responseHeaders, $this->key))
            throw new \Exception('Warlock server denied our connection attempt!');

        $this->send('sync', array('admin_key' => $this->config->admin->key));

        $response = $this->recv($payload);

        if($response == $this->protocol->getType('ok'))
            return TRUE;

        $this->disconnect(TRUE);

        return FALSE;

    }

    private function disconnect($send_close = TRUE) {

        if($this->socket) {

            if($send_close) {

                $this->closing = TRUE;

                $frame = $this->frame('', 'close');

                socket_write($this->socket, $frame, strlen($frame));

                $this->recv($payload);

            }

            socket_close($this->socket);

            $this->socket = NULL;

            return TRUE;

        }

        return FALSE;

    }

    public function connected() {

        return is_resource($this->socket);

    }

    private function send($command, $payload = NULL) {

        if(! $this->socket)
            return FALSE;

        $packet = $this->protocol->encode($command, $payload);

        $frame = $this->frame($packet, 'text');

        $len = strlen($frame);

        $bytes_sent = socket_write($this->socket, $frame, $len);

        if($bytes_sent == -1) {

            throw new \Exception('An error occured while sending to the socket');

        } elseif($bytes_sent != $len) {

            throw new \Exception($bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected');

        }

        return TRUE;

    }

    private function recv(&$payload = NULL, $tv_sec = 3, $tv_usec = 0) {

        if(! $this->socket)
            return FALSE;

        $read = array(
            $this->socket
        );

        $write = $except = NULL;

        if(socket_select($read, $write, $except, $tv_sec, $tv_usec) > 0) {

            // will block to wait server response
            $bytes_received = socket_recv($this->socket, $buf, 65536, 0);

            if($bytes_received == -1) {

                throw new \Exception('An error occured while receiving from the socket');

            } elseif($bytes_received == 0) {

                throw new \Exception('Received response of zero bytes.');

            }

            $opcode = $this->getFrame($buf, $packet);

            switch($opcode) {

                case 0:
                case 1:
                case 2:

                    return $this->protocol->decode($packet, $payload);

                case 8:

                    if($this->closing === TRUE)
                        return TRUE;

                    return $this->disconnect(TRUE);

                case 9: //PING received

                    $frame = $this->protocol->encode('pong');

                    socket_write($this->socket, $frame, strlen($frame));

                    return TRUE;

                case 10: //PONG received

                    return TRUE;

                default:

                    $this->disconnect(TRUE);

                    throw new \Exception('Invalid/Unsupported frame received from Warlock server. OPCODE=' . $opcode);

            }

        }

        return NULL;

    }

    public function isRunning() {

        if(file_exists($this->pidfile)) {

            $pid = (int)file_get_contents($this->pidfile);

            if(file_exists('/proc/' . $pid)) {

                $this->server_pid = $pid;

                return TRUE;

            }

        }

        return FALSE;

    }

    public function start($timeout = NULL) {

        if(! $this->isRunning()) {

            $php_binary = PHP_BINDIR . '/php';

            if(! file_exists($php_binary))
                throw new \Exception('The PHP CLI binary does not exist at ' . $php_binary);

            if(! is_executable($php_binary))
                throw new \Exception('The PHP CLI binary exists but is not executable!');

            $this->cmd = $php_binary . ' ' . realpath(LIBRARY_PATH . '/Warlock/Server.php');

            $env = array(
                'APPLICATION_PATH=' . APPLICATION_PATH,
                'APPLICATION_ENV=' . APPLICATION_ENV,
                'WARLOCK_EXEC=1',
                'WARLOCK_OUTPUT=file'
            );

            $this->server_pid = (int)exec(implode(' ', $env) . ' ' . sprintf("%s >> %s 2>&1 & echo $!", $this->cmd, $this->outputfile));

            if(! $this->server_pid > 0)
                return FALSE;

            $start_check = time();

            if(! $timeout)
                $timeout = $this->config->timeouts->connect;

            while(! $this->isRunning()) {

                if(time() > ($start_check + $timeout)) {

                    return FALSE;

                }

                usleep(100);

            }

        }

        return TRUE;

    }

    public function stop() {

        if($this->isRunning()) {

            $this->send('shutdown');

            if($this->recv($packet) == $this->protocol->getType('ok')) {

                $this->disconnect();

                return TRUE;

            }

        }

        return FALSE;

    }

    public function status() {

        if($this->isRunning()) {

            $this->send('status');

            if($this->recv($packet) == $this->protocol->getType('status')) {

                return $packet;

            }

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

        $response = $this->recv();

        if($response['result'] == 'error') {

            return FALSE;

        }

        return $response['job_id'];

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

        $response = $this->recv();

        if($response['result'] == 'error') {

            return FALSE;

        }

        return $response['job_id'];

    }

    public function cancel($job_id) {

        $this->send('cancel', $job_id);

        $response = $this->recv();

        if($response['result'] == 'error') {

            return FALSE;

        }

        return TRUE;

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

        return $this->send('subscribe', $subscribe);

    }

    public function trigger($event, $data = NULL) {

        $packet = array(
            'id' => $event
        );

        if($data)
            $packet['data'] = $data;

        $this->send('trigger', $packet);

        if(($response = $this->recv()) == 2) {

            return TRUE;

        }

        return FALSE;

    }

    public function wait($timeout = 0) {

        return $this->recv($payload, $timeout);

    }

    public function startService($name) {

        $this->send('enable', $name);

        if($this->recv($response) == $this->protocol->getType('start')) {

            if($response['result'] == 'ok') {

                return TRUE;

            }

        }

        return FALSE;

    }

    public function stopService($name) {

        $this->send('disable', $name);

        if($this->recv($response) == $this->protocol->getType('stop')) {

            if($response['result'] == 'ok') {

                return TRUE;

            }

        }

        return FALSE;

    }

    public function ping($client) {

        $this->send('ping', $client);

        if($this->recv($response) == $this->protocol->getType('ping')) {

            if($response['result'] == 'ok') {

                return $response;

            }

        }

        return FALSE;

    }

}

