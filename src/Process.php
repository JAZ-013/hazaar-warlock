<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

/*
 * Service Status Codes
 */
define('HAZAAR_SERVICE_ERROR', -1);

define('HAZAAR_SERVICE_INIT', 0);

define('HAZAAR_SERVICE_READY', 1);

define('HAZAAR_SERVICE_RUNNING', 2);

define('HAZAAR_SERVICE_SLEEP', 3);

define('HAZAAR_SERVICE_STOPPING', 4);

define('HAZAAR_SERVICE_STOPPED', 5);

define('HAZAAR_SCHEDULE_DELAY', 0);

define('HAZAAR_SCHEDULE_INTERVAL', 1);

define('HAZAAR_SCHEDULE_NORM', 2);

define('HAZAAR_SCHEDULE_CRON', 3);

abstract class Process extends WebSockets {

    private   $id;

    private   $key;

    protected $socket;

    protected $application;

    protected $state         = HAZAAR_SERVICE_INIT;

    protected $slept         = FALSE;

    protected $options       = array();

    protected $protocol;

    function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol) {

        parent::__construct(array(
            'warlock'
        ));

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = guid();

        $this->key = uniqid();

    }

    final function __destruct(){

        $this->disconnect(true);

    }

    public function connect($application_name, $port, $job_id, $access_key){

        if(!($this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP)))
            throw new \Exception('Unable to create TCP socket!');

        $host = '127.0.0.1';

        if(!socket_connect($this->socket, $host, $port))
            throw new \Exception('Unable to connecto to localhost:' . $port);

        /**
         * Initiate a WebSockets connection
         */
        $handshake = $this->createHandshake('/' . $application_name .'/warlock?CID=' . $this->id, $host, NULL, $this->key);

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

        return true;

        $this->send('sync', array(
            'job_id' => $job_id,
            'access_key' => $access_key,
            'process_id' => getmypid(),
            'user' => base64_encode(get_current_user())
        ));

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

    protected function setErrorHandler($methodName) {

        if(! method_exists($this, $methodName))
            throw new \Exception('Unable to set error handler.  Method does not exist!', E_ALL);

        return set_error_handler(array($this, $methodName));

    }

    protected function setExceptionHandler($methodName) {

        if(! method_exists($this, $methodName))
            throw new \Exception('Unable to set exception handler.  Method does not exist!');

        return set_exception_handler(array($this, $methodName));

    }

    protected function send($command, $payload = NULL) {

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


    /**
     * Sleep for a number of seconds.  If data is received during the sleep it is processed.  If the timeout is greater
     * than zero and data is received, the remaining timeout amount will be used in subsequent selects to ensure the
     * full sleep period is used.  If the timeout parameter is not set then the loop will just dump out after one
     * execution.
     *
     * @param int $timeout
     */
    protected function sleep($timeout = 0) {

        if(!$this->socket)
            throw new \Exception('Trying to sleep without a socket!');

        $start = microtime(true);

        $read = array(
            $this->socket
        );

        $null = NULL;

        $slept = FALSE;

        //Sleep if we are still sleeping and the timeout is not reached.  If the timeout is NULL or 0 do this process at least once.
        while($this->state < 4 && ($slept === FALSE || ($start + $timeout) >= microtime(true))) {

            $tv_sec = 0;

            $tv_usec = 0;

            if($timeout > 0) {

                $this->state = HAZAAR_SERVICE_SLEEP;

                $diff = ($start + $timeout) - microtime(true);

                $hb = $this->lastHeartbeat + $this->config['heartbeat'];

                $next = ((! $this->next || $hb < $this->next) ? $hb : $this->next);

                if($next != NULL && $next < ($diff + time()))
                    $diff = $next - time();

                if($diff > 0) {

                    $tv_sec = floor($diff);

                    $tv_usec = round(($diff - floor($diff)) * 1000000);

                } else {

                    $tv_sec = 1;

                }

            }

            if(socket_select($read, $null, $null, $tv_sec, $tv_usec) > 0) {

                $payload = NULL;

                $blksize = unpack('N', socket_read($this->socket, 4));

                dump($blksize);

                $buffer = socket_read($this->socket, $blksize);

                if($type = $this->protocol->decode($buffer, $payload))
                    $this->processCommand($type, $payload);

            }

            if($this->next > 0 && $this->next <= time())
                $this->processSchedule();

            if(($this->lastHeartbeat + $this->config['heartbeat']) <= time())
                $this->sendHeartbeat();

            $slept = true;

        }

        $this->slept = true;

        return true;

    }

    protected function processCommand($command, $payload = NULL) {

        switch($command) {

            case 'EVENT':

                if(! (array_key_exists('id', $payload) && array_key_exists($payload['id'], $this->subscriptions)))
                    return FALSE;

                try {

                    call_user_func_array(array($this, $this->subscriptions[$payload['id']]), array($payload));

                }
                catch(\Exception $e) {

                    error_log('ERROR: ' . $e->getMessage());

                }

                break;

            case 'STATUS':

                $this->sendHeartbeat();

                break;

            default:

                $this->send('DEBUG', 'Unhandled command: ' . $command);

                break;

        }

        return true;

    }

    protected function subscribe($event, $callback, $filter = NULL) {

        if(! method_exists($this, $callback))
            return FALSE;

        $this->subscriptions[$event] = $callback;

        return $this->send('subscribe', array('id' => $event, 'filter' => $filter));

    }

    protected function unsubscribe($event) {

        if(! array_key_exists($event, $this->subscriptions))
            return FALSE;

        unset($this->subscriptions[$event]);

        return $this->send('unsubscribe', array('id' => $event));

    }

    protected function trigger($event, $payload) {

        return $this->send('trigger', array('id' => $event, 'data' => $payload));

    }

}
