<?php

namespace Hazaar\Warlock\Server;

class Client extends \Hazaar\Warlock\Protocol\WebSockets {

    private $log;

    /*
     * WebSocket specific stuff
     */
    public $address;

    public $port;

    public $socket;

    public $closing = false;

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

        $this->socket = $socket;

        $this->id = uniqid();

        $this->since = time();

        if (is_resource($this->socket)) {

            $resource_type = get_resource_type($this->socket);

            if ($resource_type == 'Socket')
                socket_getpeername($this->socket, $this->address, $this->port);

            $this->log->write(W_NOTICE, "ADD: TYPE=$resource_type CLIENT=$this->id SOCKET=$this->socket");

            $this->lastContact = time();

        }

        $this->ping['wait'] = ake($options, 'wait', 15);

        $this->ping['pings'] = ake($options, 'pings', 5);

    }

    /**
     * Initiates a WebSocket client handshake
     *
     * @param mixed $socket
     * @param mixed $request
     * @return boolean
     */
    public function initiateHandshake($request) {

        if(!($headers = $this->parseHeaders($request))){

            $this->log->write(W_WARN, 'Unable to parse request while initiating WebSocket handshake!');

            return false;

        }

        if (!(array_key_exists('connection', $headers) && preg_match('/upgrade/', strtolower($headers['connection']))))
            return false;

        $responseCode = $this->acceptHandshake($headers, $responseHeaders, NULL, $results);

        if (!(array_key_exists('get', $headers) && $responseCode === 101)) {

            $responseHeaders['Connection'] = 'close';

            $responseHeaders['Content-Type'] = 'text/text';

            $body = $responseCode . ' ' . http_response_text($responseCode);

            $response = $this->httpResponse($responseCode, $body, $responseHeaders);

            $this->log->write(W_WARN, "Handshake failed with code $body");

            @socket_write($this->socket, $response, strlen($response));

            return false;

        }

        $this->log->write(W_NOTICE, "Initiating WebSockets handshake");

        if (!($this->id = $results['url']['CID']))
            return false;

        if(array_key_exists('UID', $results['url'])){

            $this->username = base64_decode($results['url']['UID']);

            if ($this->username != NULL)
                $this->log->write(W_NOTICE, "USER: $this->username");

        }

        $response = $this->httpResponse($responseCode, null, $responseHeaders);

        $bytes = strlen($response);

        $result = @socket_write($this->socket, $response, $bytes);

        if($result === false || $result !== $bytes)
            return false;

        $init_frame = $this->frame(json_encode(\Hazaar\Application\Protocol::$typeCodes), 'text', false);

        //If this is NOT a Warlock process request (ie: it's a browser) send the protocol init frame!
        if(!(array_key_exists('x-warlock-php', $headers) && $headers['x-warlock-php'] === 'true')){

            $this->log->write(W_DEBUG, 'Sending Hazaar\Application\Protocol initialisation frame');

            $this->write($init_frame);

        }

        if(array_key_exists('x-warlock-admin-key', $headers)){

            $payload = (object)array('admin_key' => base64_decode($headers['x-warlock-admin-key']));

            if(!$this->commandSync($payload, false))
                return false;

        }elseif(array_key_exists('x-warlock-job-id', $headers) && array_key_exists('x-warlock-access-key', $headers)){

            $payload = (object)array(
                'client_id' => $this->id,
                'job_id' => $headers['x-warlock-job-id'],
                'access_key' => base64_decode($headers['x-warlock-access-key'])
            );

            if(!$this->commandSync($payload, false))
                return false;

        }

        $this->log->write(W_NOTICE, 'WebSockets handshake successful!');

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

    public function recv($buf){

        //Record this time as the last time we received data from the client
        $this->lastContact = time();

        /**
         * Sometimes we can get multiple frames in a single buffer so we cycle through
         * them until they are all processed.  This will even allow partial frames to be
         * added to the client frame buffer.
         */
        while($frame = $this->processFrame($buf)) {

            $this->log->write(W_DECODE, "CLIENT<-PACKET: " . $frame);

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

                    $this->log->write(W_ERR, 'An error occurred processing the command TYPE: ' . $type);

                    $this->send('error', array(
                        'reason' => $e->getMessage(),
                        'command' => $type
                    ));

                }

            } else {

                $reason = Master::$protocol->getLastError();

                $this->log->write(W_ERR, "Protocol error: $reason");

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

        $this->log->write(W_DECODE, "CLIENT->PACKET: $packet");

        $frame = $this->frame($packet, 'text', false);

        return $this->write($frame);

    }

    private function write($frame){

        if (!is_resource($this->socket))
            return false;

        $len = strlen($frame);

        $this->log->write(W_DEBUG, "CLIENT->SOCKET: BYTES=$len SOCKET=$this->socket");

        $this->log->write(W_DECODE2, "CLIENT->FRAME: " . implode(' ', $this->hexString($frame)));

        $bytes_sent = @socket_write($this->socket, $frame, $len);

        if ($bytes_sent === false) {

            $this->log->write(W_WARN, 'An error occured while sending to the client. Could be disconnected.');

            return false;

        } elseif ($bytes_sent != $len) {

            $this->log->write(W_ERR, $bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected');

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

        $this->log->write(W_DEBUG, 'DISCONNECT: CLIENT=' . $this->id . ' SOCKET=' . $this->socket);

        $this->subscriptions = array();

        $this->log->write(W_DEBUG, "CLIENT_SOCKET_CLOSE: SOCKET=" . $this->socket);

        Master::$instance->removeClient($this->socket);

        socket_close($this->socket);

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

        $this->log->write(W_DECODE2, "CLIENT<-FRAME: " . implode(' ', $this->hexString($frameBuffer)));

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

            $this->log->write(W_ERR, 'Bad frame received from client. Disconnecting.');

            $this->disconnect($this->socket);

            return false;

        } elseif ($opcode === true) {

            $this->log->write(W_WARN, 'Fragment frame received.');

            $this->payloadBuffer .= $payload;

            return false;

        } elseif ($opcode === -1) {

            $this->frameBuffer = $frameBuffer;

            return false;

        }

        $this->log->write(W_DECODE2, "OPCODE: $opcode");

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

                    $this->log->write(W_DEBUG, "WEBSOCKET_CLOSE: HOST=$this->address:$this->port");

                    $this->closing = true;

                    $frame = $this->frame('', 'close', false);

                    @socket_write($this->socket, $frame, strlen($frame));

                    if($this->type === 'client' && ($count = count($this->jobs)) > 0){

                        $this->log->write(W_NOTICE, 'Disconnected WebSocket client has '
                            . $count . ' running/pending child jobs');

                        foreach($this->jobs as $job){

                            if($job->detach !== true)
                                $job->status = STATUS_CANCELLED;

                        }

                    }

                    $this->disconnect();

                }

                return false;

            case 9 : //Ping

                $this->log->write(W_DEBUG, "WEBSOCKET_PING: HOST=$this->address:$this->port");

                $frame = $this->frame('', 'pong', false);

                @socket_write($this->socket, $frame, strlen($frame));

                return false;

            case 10 : //Pong

                $this->log->write(W_DEBUG, "WEBSOCKET_PONG: HOST=$this->address:$this->port");

                $this->pong($payload);

                return false;

            default : //Unknown!

                $this->log->write(W_DEBUG, "DISCONNECT: REASON=unknown opcode HOST=$this->address:$this->port");

                $this->disconnect();

                return false;

        }

        return $payload;

    }

    private function processCommand($command, $payload = null){

        if (!$command)
            return false;

        $this->log->write(W_DEBUG, "CLIENT<-COMMAND: $command CLIENT=$this->id");

        switch($command){

            case 'NOOP':

                $this->log->write(W_INFO, 'NOOP: ' . print_r($payload, true));

                return true;

            case 'OK':

                if($payload)
                    $this->log->write(W_INFO, $payload);

                return true;

            case 'ERROR':

                $this->log->write(W_ERR, $payload);

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

                    $this->log->write(W_INFO, 'PONG received in ' . $trip_ms . 'ms');

                }else{

                    $this->log->write(W_WARN, 'PONG received with invalid payload!');

                }

                break;

            case 'LOG':

                return $this->commandLog($payload);

            case 'DEBUG':

                $this->log->write(W_DEBUG, ake($payload, 'data'));

                return true;

            case 'STATUS' :

                if($payload)
                    return $this->commandStatus($payload);

            default:

                return Master::$instance->processCommand($this, $command, $payload);

        }

        return false;

    }

    private function commandSync(\stdClass $payload, $acknowledge = true){

        $this->log->write(W_DEBUG, "SYNC: CLIENT_ID=$this->id OFFSET=$this->offset");

        if (property_exists($payload, 'admin_key')){

            if(!Master::$instance->authorise($this, $payload->admin_key) && $acknowledge === true) {

                $this->log->write(W_WARN, 'Warlock control rejected to client ' . $this->id);

                $this->send('ERROR');

            }

            if($acknowledge === true) $this->send('OK');

            return true;

        }elseif(property_exists($payload, 'job_id')
            && $payload->client_id === $this->id){

            if(!Master::$instance->authorise($this, $payload->access_key, $payload->job_id)){

                $this->log->write(W_ERR, 'Service tried to sync with bad access key!', $payload->job_id);

                return false; //A bad access key will drop the service.

            }

            if($acknowledge === true) $this->send('OK');

            return true;

        }

        $this->log->write(W_ERR, 'Client requested bad sync!');

        if($acknowledge) $this->send('ERROR');

        return false;

    }

    private function commandStatus(\stdClass $payload = null) {

        if($this->type !== 'service'){

            $this->log->write(W_WARN, 'Client sent status but client is not a service!', $this->address);

            throw new \Exception('Status only allowed for services!');

        }

        $job = ake($this->jobs, $payload->job_id);

        if(!$job){

            $this->log->write(W_WARN, 'Service status received for client with no job ID');

            throw new \Exception('Service has no running job!');

        }

        return true;

    }

    private function commandSubscribe($event_id, $filter = NULL) {

        $this->log->write(W_NOTICE, "CLIENT->SUBSCRIBE: EVENT=$event_id CLIENT=$this->id");

        $this->subscriptions[] = $event_id;

        Master::$instance->subscribe($this, $event_id, $filter);

        return true;

    }

    public function commandUnsubscribe($event_id) {

        $this->log->write(W_DEBUG, "CLIENT->UNSUBSCRIBE: EVENT=$event_id CLIENT=$this->id");

        if(($index = array_search($event_id, $this->subscriptions)) !== false)
            unset($this->subscriptions[$index]);

        if(!Master::$instance->unsubscribe($this, $event_id))
            throw new \Exception('Unable to subscribe');

        return true;

    }

    public function commandTrigger($event_id, $data, $echo_client = true) {

        $this->log->write(W_NOTICE, "CLIENT->TRIGGER: NAME=$event_id CLIENT=$this->id ECHO=" . strbool($echo_client));

        Master::$instance->trigger($event_id, $data, ($echo_client === false ? $this->id : null));

        return true;

    }

    private function commandLog(\stdClass $payload){

        if(!property_exists($payload, 'msg'))
            throw new \Exception('Unable to write to log without a log message!');

        $level = ake($payload, 'level', W_INFO);

        $name = ake($payload, 'name');

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

            $this->log->write(W_WARN, "Client $this->id is not subscribe to event $event_id");

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

            $this->log->write(W_WARN, 'Disconnecting client due to lack of PONG!');

            $this->disconnect();

            return false;

        }

        $this->ping['last'] = time();

        $this->log->write(W_DEBUG, 'CLIENT->PING: ATTEMPTS=' . $this->ping['attempts'] . ' LAST=' . date('c', $this->ping['last']));

        return $this->write($this->frame('', 'ping', false));

    }

    public function pong(){

        $this->ping['attempts'] = 0;

        $this->ping['last'] = 0;

    }

}
