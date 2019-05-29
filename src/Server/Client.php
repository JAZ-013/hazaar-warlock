<?php

namespace Hazaar\Warlock\Server;

class Client extends \Hazaar\Warlock\Protocol\WebSockets implements CommInterface {

    public $log;

    /**
     * @var string
     */
    public $address;

    /**
     * @var integer
     */
    public $port;

    /**
     * @var resource
     */
    public $socket;

    /**
     * @var boolean
     */
    public $closing = false;

    /**
     * Buffer for fragmented frames
     * @var string
     */
    public $frameBuffer = NULL;

    /**
     * Buffer for payloads split over multiple frames
     * @var string
     */
    public $payloadBuffer = NULL;

    /*
     * Warlock specific stuff
     */
    public $id;

    public $type = 'CLIENT';  //Possible types are 'CLIENT', 'SERVICE' or 'ADMIN'.

    public $name;

    public $username;

    public $status;

    public $since;

    public $lastContact = 0;

    public $ping = array(
        'attempts' => 0,
        'last' => 0,
        'retry' => 5,
        'retries' => 3
    );

    /**
     * Any detected time offset. This doesn't need to be exact so we don't bother worrying about latency.
     * @var int
     */
    public $offset = NULL;

    /**
     * This is an array of event_id and socket pairs
     * @var array
     */
    public $subscriptions = array();

    /**
     * If the client has any child jobs
     * @var mixed
     */
    public $jobs = array();

    function __construct($socket = NULL, $options = array()) {

        parent::__construct(array('warlock'));

        $this->log = new \Hazaar\Warlock\Server\Logger();

        $this->id = uniqid();

        $this->since = time();

        if($socket === null)
            return;

        $this->socket = $socket;

        $this->name = 'SOCKET#' . intval($socket);

        if (is_resource($this->socket)) {

            if($peer = stream_socket_get_name($this->socket, true)){

                list($this->address, $this->port) = explode(':', $peer);

                $this->log->write(W_DEBUG, $this->type . "<-CREATE: HOST=$this->address PORT=$this->port", $this->name);

            }

            $this->lastContact = time();

        }

        $this->ping['wait'] = ake($options, 'pingWait', 15);

        $this->ping['pings'] = ake($options, 'pingCount', 5);

    }

    function __destruct(){

        $this->log->write(W_DEBUG, $this->type . "->DESTROY: HOST=$this->address PORT=$this->port", $this->name);

    }

    /**
     * Initiates a WebSocket client handshake
     *
     * @param mixed $socket
     * @param mixed $request
     * @return boolean
     */
    public function processHandshake($request) {

        if(!($headers = $this->parseHeaders($request))){

            $this->log->write(W_WARN, 'Unable to parse request while initiating WebSocket handshake!', $this->name);

            return false;

        }

        if (!(array_key_exists('connection', $headers) && preg_match('/upgrade/', strtolower($headers['connection']))))
            return false;

        $this->log->write(W_DEBUG, "WEBSOCKETS<-HANDSHAKE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        $responseCode = $this->acceptHandshake($headers, $responseHeaders, NULL, $results);

        if (!(array_key_exists('get', $headers) && $responseCode === 101)) {

            $responseHeaders['Connection'] = 'close';

            $responseHeaders['Content-Type'] = 'text/text';

            $body = $responseCode . ' ' . http_response_text($responseCode);

            $response = $this->httpResponse($responseCode, $body, $responseHeaders);

            $this->log->write(W_WARN, "Handshake failed with code $body", $this->name);

            @fwrite($this->socket, $response, strlen($response));

            return false;

        }

        if (!($this->id = $results['url']['CID']))
            return false;

        if(array_key_exists('UID', $results['url'])){

            $this->username = base64_decode($results['url']['UID']);

            if ($this->username != NULL)
                $this->log->write(W_NOTICE, "USER: $this->username", $this->name);

        }

        $this->log->write(W_DEBUG, "WEBSOCKETS->ACCEPT: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        $response = $this->httpResponse($responseCode, null, $responseHeaders);

        $bytes = strlen($response);

        $result = @fwrite($this->socket, $response, $bytes);

        if($result === false || $result !== $bytes)
            return false;

        $init_packet = json_encode(\Hazaar\Warlock\Protocol::$typeCodes);

        if(Master::$protocol->encoded())
            $init_packet = base64_encode($init_packet);

        $init_frame = $this->frame($init_packet, 'text', false);

        //If this is NOT a Warlock process request (ie: it's a browser) send the protocol init frame!
        if(!(array_key_exists('x-warlock-php', $headers) && $headers['x-warlock-php'] === 'true')){

            $this->log->write(W_DEBUG, $this->type . "->INIT: HOST=$this->address POST=$this->port CLIENT=$this->id", $this->name);

            $this->write($init_frame);

        }

        if(array_key_exists('x-warlock-access-key', $headers)){

            $payload = (object)array(
                'client_id' => $this->id,
                'type' => $type = ake($headers, 'x-warlock-client-type', 'admin'),
                'access_key' => base64_decode($headers['x-warlock-access-key'])
            );

            if(!$this->commandSync($payload))
                return false;

            if($type === 'service')
                $this->send('OK');

        }

        $this->log->write(W_NOTICE, "WebSockets connection from $this->address:$this->port", $this->name);

        return true;

    }

    /**
     * Generate an HTTP response message
     *
     * @param mixed $code HTTP response code
     * @param mixed $body The response body
     * @param mixed $headers Additional headers
     *
     * @return \boolean|string
     */
    private function httpResponse($code, $body = NULL, $headers = array()) {

        if (!is_array($headers))
            return false;

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

    /**
     * Overridden method from WebSocket class to check the requested WebSocket URL is valid.
     *
     * @param mixed $url
     * @return \array|boolean
     */
    protected function checkRequestURL($url) {

        $parts = parse_url($url);

        // Check that a path was actually sent
        if (!array_key_exists('path', $parts))
            return false;

        // Check that the path is correct based on the APPLICATION_NAME constant
        if ($parts['path'] != '/' . APPLICATION_NAME . '/warlock')
            return false;

        // Check to see if there is a query part as this should contain the CID
        if (!array_key_exists('query', $parts))
            return false;

        // Get the CID
        parse_str($parts['query'], $query);

        if (!array_key_exists('CID', $query))
            return false;

        return $query;

    }

    public function recv(&$buf){

        //Record this time as the last time we received data from the client
        $this->lastContact = time();

        /**
         * Sometimes we can get multiple frames in a single buffer so we cycle through
         * them until they are all processed.  This will even allow partial frames to be
         * added to the client frame buffer.
         */
        while($frame = $this->processFrame($buf)) {

            $this->log->write(W_DECODE, $this->type . "<-PACKET: " . $frame, $this->name);

            $payload = null;

            $time = null;

            $type = Master::$protocol->decode($frame, $payload, $time);

            if ($type) {

                $this->offset = (time() - $time);

                try{

                    if (!$this->processCommand($type, $payload, $time))
                        throw new \Exception('Negative response returned while processing command!');

                }
                catch(\Exception $e){

                    $this->log->write(W_ERR, 'An error occurred processing the command: ' . $type, $this->name);

                    $this->send('error', array(
                        'reason' => $e->getMessage(),
                        'command' => $type
                    ));

                }

            } else {

                $reason = Master::$protocol->getLastError();

                $this->log->write(W_ERR, "Protocol error: $reason", $this->name);

                $this->send('error', array(
                    'reason' => $reason
                ));

            }

        }

    }

    public function send($command, $payload = NULL) {

        if (!is_string($command))
            return false;

        $packet = Master::$protocol->encode($command, $payload); //Override the timestamp.

        $this->log->write(W_DECODE, $this->type . "->PACKET: $packet", $this->name);

        $frame = $this->frame($packet, 'text', false);

        return $this->write($frame);

    }

    private function write($frame){

        if (!is_resource($this->socket))
            return false;

        $len = strlen($frame);

        $this->log->write(W_DECODE2, $this->type . "->FRAME: " . implode(' ', $this->hexString($frame)), $this->name);

        $this->log->write(W_DEBUG, $this->type . "->SOCKET: BYTES=$len HOST=$this->address PORT=$this->port", $this->name);

        $bytes_sent = @fwrite($this->socket, $frame, $len);

        if ($bytes_sent === false) {

            $this->log->write(W_WARN, 'An error occured while sending to the client. Could be disconnected.', $this->name);

            $this->disconnect();

            return false;

        } elseif ($bytes_sent != $len) {

            $this->log->write(W_ERR, $bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected', $this->name);

            $this->disconnect();

            return false;

        }

        return true;

    }

    /**
     * Process a socket client disconnect
     *
     * @param mixed $socket
     */
    public function disconnect() {

        $this->subscriptions = array();

        Master::$instance->removeClient($this->socket);

        stream_socket_shutdown($this->socket, STREAM_SHUT_RDWR);

        $this->log->write(W_DEBUG, $this->type . "->CLOSE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        fclose($this->socket);

    }

    /**
     * Processes a client data frame.
     *
     * @param mixed $frameBuffer
     *
     * @return mixed
     */
    private function processFrame(&$frameBuffer) {

        if ($this->frameBuffer) {

            $frameBuffer = $this->frameBuffer . $frameBuffer;

            $this->frameBuffer = NULL;

            return $this->processFrame($frameBuffer);

        }

        if (!$frameBuffer)
            return false;

        $this->log->write(W_DECODE2, $this->type . "<-FRAME: " . implode(' ', $this->hexString($frameBuffer)), $this->name);

        $opcode = $this->getFrame($frameBuffer, $payload);

        /**
         * If we get an opcode that equals false then we got a bad frame.
         *
         * If we get an opcode actually equals true, then the FIN flag was not set so this is a fragmented
         * frame and there wil be one or more coninuation frames.  So, we return false if there are no more
         * frames to process, or true if there are already more frames in the buffer to process.
         *
         * If we get a opcode of -1 then we received only part of the frame and there is more data
         * required to complete the frame.
         */
        if ($opcode === false) {

            $this->log->write(W_ERR, 'Bad frame received from client. Disconnecting.', $this->name);

            $this->disconnect();

            return false;

        } elseif ($opcode === true) {

            $this->log->write(W_WARN, "Websockets fragment frame received from $this->address:$this->port", $this->name);

            $this->payloadBuffer .= $payload;

            return false;

        } elseif ($opcode === -1) {

            $this->frameBuffer = $frameBuffer;

            return false;

        }

        $this->log->write(W_DECODE2, $this->type . "<-OPCODE: $opcode", $this->name);

        //Save any leftover frame data in the client framebuffer because we got more than a whole frame)
        if (strlen($frameBuffer) > 0) {

            $this->frameBuffer = $frameBuffer;

            $frameBuffer = '';

        }

        //Check the WebSocket OPCODE and see if we need to do any internal processing like PING/PONG, CLOSE, etc.
        switch ($opcode) {

            case 0 : //If the opcode is 0, then this is our FIN continuation frame.

                //If we have data in the payload buffer (we absolutely should) then retrieve it here.
                if (!$this->payloadBuffer)
                    $this->log(W_WARN, 'Got finaly continuation frame but there is no payload in the buffer!?');

                $payload = $this->payloadBuffer . $payload;

                $this->payloadBuffer = '';

                break;

            case 1 : //Text frame
            case 2 : //Binary frame

                //These are our normal frame types which will already be processed into $payload.

                break;

            case 8 : //Close frame

                if($this->closing === false){

                    $this->log->write(W_DEBUG, "WEBSOCKET<-CLOSE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

                    $this->closing = true;

                    $frame = $this->frame('', 'close', false);

                    @fwrite($this->socket, $frame, strlen($frame));

                    if($this->type === 'CLIENT' && ($count = count($this->jobs)) > 0){

                        $this->log->write(W_NOTICE, 'Disconnected WebSocket client has '
                            . $count . ' running/pending child jobs', $this->name);

                        foreach($this->jobs as $job){

                            if($job->detach !== true)
                                $job->status = STATUS_CANCELLED;

                        }

                    }

                    $this->log->write(W_NOTICE, "Websockets connection closed to $this->address:$this->port", $this->name);

                    $this->disconnect();

                }

                return false;

            case 9 : //Ping

                $this->log->write(W_DEBUG, "WEBSOCKET<-PING: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

                $frame = $this->frame('', 'pong', false);

                @fwrite($this->socket, $frame, strlen($frame));

                return false;

            case 10 : //Pong

                $this->log->write(W_DEBUG, "WEBSOCKET<-PONG: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

                $this->pong($payload);

                return false;

            default : //Unknown!

                $this->log->write(W_ERR, "Bad opcode received on Websocket connection from $this->address:$this->port", $this->name);

                $this->disconnect();

                return false;

        }

        return $payload;

    }

    protected function processCommand($command, $payload = null){

        if (!$command)
            return false;

        $this->log->write(W_DEBUG, $this->type . "<-COMMAND: $command HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        switch($command){

            case 'NOOP':

                $this->log->write(W_INFO, 'NOOP: ' . print_r($payload, true), $this->name);

                return true;

            case 'OK':

                if($payload)
                    $this->log->write(W_INFO, $payload, $this->name);

                return true;

            case 'ERROR':

                $this->log->write(W_ERR, $payload, $this->name);

                return true;

            case 'SYNC':

                return $this->commandSync($payload);

            case 'SUBSCRIBE' :

                $filter = (property_exists($payload, 'filter') ? $payload->filter : NULL);

                return $this->commandSubscribe($payload->id, $filter);

            case 'UNSUBSCRIBE' :

                return $this->commandUnsubscribe($payload->id);

            case 'TRIGGER' :

                return $this->commandTrigger($payload->id, ake($payload, 'data'), ake($payload, 'echo', false));

            case 'PING' :

                return $this->send('pong', $payload);

            case 'PONG':

                if(is_int($payload)){

                    $trip_ms = (microtime(true) - $payload) * 1000;

                    $this->log->write(W_INFO, 'PONG received in ' . $trip_ms . 'ms', $this->name);

                }else{

                    $this->log->write(W_WARN, 'PONG received with invalid payload!', $this->name);

                }

                break;

            case 'LOG':

                return $this->commandLog($payload);

            case 'DEBUG':

                $this->log->write(W_DEBUG, ake($payload, 'data'), $this->name);

                return true;

            case 'STATUS' :

                if($payload)
                    return $this->commandStatus($payload);

            default:

                return Master::$instance->processCommand($this, $command, $payload);

        }

        return false;

    }

    private function commandSync(\stdClass $payload){

        $this->log->write(W_DEBUG, $this->type . "<-SYNC: OFFSET=$this->offset HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        if (!property_exists($payload, 'access_key'))
            return false;

        if(!Master::$instance->authorise($this, $payload->access_key)) {

            $this->log->write(W_WARN, 'Warlock control rejected to client ' . $this->id, $this->name);

            $this->send('ERROR');

            return false;

        }

        $this->send('OK');

        if($this->type !== $payload->type){

            $type = strtoupper($payload->type);

            $this->log->write(W_NOTICE, "Client type changed from $this->type to $type.", $this->name);

            $this->type = $type;

        }

        return true;

    }

    private function commandStatus(\stdClass $payload = null) {

        if($this->type !== 'SERVICE'){

            $this->log->write(W_WARN, 'Client sent status but client is not a service!', $this->address . ':' . $this->port);

            throw new \Exception('Status only allowed for services!');

        }

        $this->status = $payload;

        return true;

    }

    private function commandSubscribe($event_id, $filter = NULL) {

        $this->log->write(W_DEBUG, $this->type . "<-SUBSCRIBE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        $this->subscriptions[] = $event_id;

        Master::$instance->subscribe($this, $event_id, $filter);

        return true;

    }

    public function commandUnsubscribe($event_id) {

        $this->log->write(W_DEBUG, $this->type . "<-UNSUBSCRIBE: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        if(($index = array_search($event_id, $this->subscriptions)) !== false)
            unset($this->subscriptions[$index]);

        return Master::$instance->unsubscribe($this, $event_id);

    }

    public function commandTrigger($event_id, $data, $echo_client = true) {

        $this->log->write(W_DEBUG, $this->type . "<-TRIGGER: HOST=$this->address PORT=$this->port CLIENT=$this->id", $this->name);

        return Master::$instance->trigger($event_id, $data, ($echo_client === false ? $this->id : null));

    }

    private function commandLog(\stdClass $payload){

        if(!property_exists($payload, 'msg'))
            throw new \Exception('Unable to write to log without a log message!');

        $level = ake($payload, 'level', W_INFO);

        $name = ake($payload, 'name', $this->name);

        if(is_array($payload->msg)){

            foreach($payload->msg as $msg)
                $this->commandLog((object)array('level' => $level, 'msg' => $msg, 'name' => $name));

        }else{

            $this->log->write($level, ake($payload, 'msg', '--'), $name);

        }

        return true;

    }

    public function sendEvent($event_id, $trigger_id, $data) {

        if (!in_array($event_id, $this->subscriptions)) {

            $this->log->write(W_WARN, "Client $this->id is not subscribe to event $event_id", $this->name);

            return false;

        }

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(true),
            'data' => $data
        );

        return $this->send('EVENT', $packet);

    }

    public function ping(){

        if((time() - $this->ping['wait']) < $this->ping['last'])
            return false;

        $this->ping['attempts']++;

        if($this->ping['attempts'] > $this->ping['pings']){

            $this->log->write(W_WARN, 'Disconnecting client due to lack of PONG!', $this->name);

            $this->disconnect();

            return false;

        }

        $this->ping['last'] = time();

        $this->log->write(W_DEBUG, 'WEBSOCKET->PING: ATTEMPTS=' . $this->ping['attempts'] . ' LAST=' . date('c', $this->ping['last']), $this->name);

        return $this->write($this->frame('', 'ping', false));

    }

    public function pong(){

        $this->ping['attempts'] = 0;

        $this->ping['last'] = 0;

    }

}
