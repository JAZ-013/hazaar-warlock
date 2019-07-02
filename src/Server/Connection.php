<?php

namespace Hazaar\Warlock\Server;

define('WARLOCK_CONN_OFFLINE', 0);

define('WARLOCK_CONN_INIT', 1);

define('WARLOCK_CONN_HANDSHAKE', 2);

define('WARLOCK_CONN_ONLINE', 3);

define('WARLOCK_CONN_CLOSING', 4);

class Connection extends \Hazaar\Warlock\Protocol\WebSockets implements CommInterface {

    //WebSocket Security Key
    private $key;

    /**
     * @var string
     */
    public $address;

    /**
     * @var integer
     */
    public $port;

    /**
     * @var resource|array
     *
     * Read stream.
     *
     * In the case of a socket, this is also the write stream.
     *
     * In the case of pipes, this is an array of write/read/error streams.
     */
    public $stream;

    private $name;

    /**
     * The connection is online and WebSocket protocol has been negotiated successfully
     * @var boolean
     */
    private $status = WARLOCK_CONN_OFFLINE;

    /**
     * Indicates that this is a direct connection and does not use WebSockets.
     *
     * @var bool
     */
    private $direct = false;

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
    private $node;

    public $log;

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
     *
     * @var resource|Node
     */
    public $offset = 0;

    function __construct($stream_or_node = null, $options = array()) {

        parent::__construct(array('warlock'));

        $this->log = Master::$instance->log;

        $this->ping['wait'] = ake($options, 'pingWait', 15);

        $this->ping['pings'] = ake($options, 'pingCount', 5);

        $this->attach($stream_or_node);

    }

    function __destruct(){

        $this->log->write(W_DEBUG, "CONNECTION->DESTROY: HOST=$this->address PORT=$this->port", $this->name);

    }

    public function setNode(Node $node){

        $this->node = $node;

    }

    public function getReadStream(){

        if(is_resource($this->stream))
            return $this->stream;

        if(is_array($this->stream) && is_resource($this->stream[1]))
            return $this->stream[1];

        return false;

    }

    public function getWriteStream(){

        if(is_resource($this->stream))
            return $this->stream;

        if(is_array($this->stream) && is_resource($this->stream[0]))
            return $this->stream[0];

        return false;

    }

    public function connect($address, $port, $headers = null){

        if(!($address && $port > 0))
            return false;

        if(is_resource($this->stream))
            return false;

        $this->log->write(W_INFO, 'Connecting to peer at ' . $address . ':' . $port);

        $this->stream = stream_socket_client('tcp://' . $address . ':' . $port, $errno, $errstr, 0, STREAM_CLIENT_CONNECT | STREAM_CLIENT_ASYNC_CONNECT);

        if(!$this->stream){

            $this->log->write(W_ERR, 'Error #' . $errno . ': ' . $errstr);

            return false;

        }

        $this->status = WARLOCK_CONN_INIT;

        return true;

    }

    public function initHandshake($headers = array()){

        if($this->status !== WARLOCK_CONN_INIT)
            return false;

        $this->key = uniqid();

        $handshake = $this->createHandshake('/' . APPLICATION_NAME . '/warlock', $this->address, null, $this->key, $headers);

        $this->log->write(W_DEBUG, "WEBSOCKETS->HANDSHAKE: HOST=$this->address PORT=$this->port", $this->name);

        fwrite($this->stream, $handshake);

        $this->status = WARLOCK_CONN_HANDSHAKE;

        return true;

    }

    public function attach($stream){

        if($stream instanceof Node){

            $this->node = $stream;

            $this->node->conn = $this;

            return true;

        }elseif($this->name !== null)
            return false;

        $stream_id = null;

        $this->stream = $stream;

        $this->since = time();

        if(is_resource($stream)){ //Socket

            $stream_id = intval($stream);

            $meta = stream_get_meta_data($stream);

            switch($meta['stream_type']){

                case 'tcp_socket/ssl':

                    if(($peer = stream_socket_get_name($this->stream, true)) === false)
                        return false;

                    list($this->address, $this->port) = explode(':', $peer);

                    $this->log->write(W_DEBUG, "CONNECTION->CREATE: HOST=$this->address PORT=$this->port", $this->name);

                    break;

                default:

                    $this->log->write(W_WARN, "Unknown stream type: " . $meta['stream_type'], $this->name);

            }

        }elseif(is_array($stream) && is_resource($stream[0])){ //Pipes

            $stream_id = intval($stream[1]); //We use the read stream id

            $this->log->write(W_DEBUG, "CONNECTION->CREATE: PIPES COUNT=" . count($stream), $this->name);

            $this->status = WARLOCK_CONN_ONLINE;

            $this->direct = true;

        }

        $this->log->write(W_DEBUG, "CONNECTION->ATTACH: STREAM=$stream_id");

        $this->name = 'STREAM#' . $stream_id;

        return true;

    }

    public function attached(){

        return is_resource($this->stream);

    }

    public function connected(){

        if(($connected = (is_resource($this->stream) && stream_socket_get_name($this->stream, true) !== false))
            && $this->status === WARLOCK_CONN_INIT)
            $this->attach($this->stream);

        return $connected;

    }

    public function online(){

        return $this->status === WARLOCK_CONN_ONLINE;

    }

    /**
     * Process a socket client disconnect
     *
     * @param mixed $socket
     */
    public function disconnect($remove_node = false) {

        if(!is_resource($this->stream))
            return false;

        if($this->status === WARLOCK_CONN_ONLINE){

            $this->log->write(W_DEBUG, "WEBSOCKET->CLOSE: HOST=$this->address PORT=$this->port", $this->name);

            $frame = $this->frame('', 'close', false);

            @fwrite($this->stream, $frame, strlen($frame));

        }

        Master::$instance->removeConnection($this);

        if($this->node){

            Master::$cluster->removeNode($this->node);

            if($remove_node === true)
                $this->node = null;

        }

        stream_socket_shutdown($this->stream, STREAM_SHUT_RDWR);

        $this->log->write(W_DEBUG, "CONNECTION->CLOSE: HOST=$this->address PORT=$this->port", $this->name);

        fclose($this->stream);

        $this->stream = $this->name = null;

        $this->status = WARLOCK_CONN_OFFLINE;

        return true;

    }

    /**
     * Complete an outbound WebSocket handshake
     *
     * @param mixed $buf
     * @throws \Exception
     * @return boolean|null
     */
    private function completeHandshake(&$buf){

        if(($pos = strpos($buf, "\r\n\r\n")) === false)
            return null;

        if(!($response = $this->parseHeaders(substr($buf, 0, $pos))))
            return false;

        $buf = substr($buf, $pos + 4);

        if($response['code'] !== 101)
            throw new \Exception('Walock server returned status: ' . $response['code'] . ' ' . $response['status']);

        if(!$this->acceptHandshake($response, $responseHeaders, $this->key))
            throw new \Exception('Warlock server denied our connection attempt!');

        $this->log->write(W_DEBUG, "CONNECTION<-ACCEPT: HOST=$this->address PORT=$this->port", $this->name);

        $this->status = WARLOCK_CONN_ONLINE;

        if($this->node)
            return $this->node->init($response);

        return true;

    }

    /**
     * Initiates a WebSocket client handshake
     *
     * @param mixed $socket
     * @param mixed $request
     * @return boolean
     */
    public function processHandshake(&$request) {

        if(!($headers = $this->parseHeaders($request))){

            $this->log->write(W_WARN, 'Unable to parse request while initiating WebSocket handshake!', $this->name);

            return false;

        }

        if (!(array_key_exists('connection', $headers) && preg_match('/upgrade/', strtolower($headers['connection']))))
            return false;

        $this->log->write(W_DEBUG, "CONNECTION<-HANDSHAKE: HOST=$this->address PORT=$this->port", $this->name);

        $responseCode = $this->acceptHandshake($headers, $responseHeaders, NULL, $results);

        if (!(array_key_exists('get', $headers) && $responseCode === 101)) {

            $responseHeaders['Connection'] = 'close';

            $responseHeaders['Content-Type'] = 'text/text';

            $body = $responseCode . ' ' . http_response_text($responseCode);

            $response = $this->httpResponse($responseCode, $body, $responseHeaders);

            $this->log->write(W_WARN, "Handshake failed with code $body", $this->name);

            @fwrite($this->stream, $response, strlen($response));

            return false;

        }

        if(!($node = Master::$cluster->createNode($this, $headers)))
            return false;

        $this->log->write(W_DEBUG, "CONNECTION->ACCEPT: HOST=$this->address PORT=$this->port", $this->name);

        $responseHeaders['X-WARLOCK-PEER-NAME'] = $node->name;

        $response = $this->httpResponse($responseCode, null, $responseHeaders);

        $bytes = strlen($response);

        $result = @fwrite($this->stream, $response, $bytes);

        if($result === false || $result !== $bytes)
            return false;

        $this->status = WARLOCK_CONN_ONLINE;

        $this->node = $node;

        $this->log->write(W_NOTICE, "WebSockets connection from $this->address:$this->port");

        return $node->init($headers);

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

        return true;

    }

    public function send($packet){

        $this->log->write(W_DECODE, "CONNECTION->PACKET: " . $packet, $this->name);

        return $this->write(($this->direct ? $packet . "\n": $this->frame($packet, 'text', false)));

    }

    private function write($frame){

        $stream = $this->getWriteStream();

        if(!is_resource($stream))
            return false;

        $len = strlen($frame);

        $this->log->write(W_DECODE2, "CONNECTION->FRAME: " . implode(' ', $this->hexString($frame)), $this->name);

        $desc = is_resource($this->stream) ? "HOST=$this->address PORT=$this->port" : "PIPE";

        $this->log->write(W_DEBUG, "CONNECTION->SOCKET: BYTES=$len $desc", $this->name);

        $bytes_sent = @fwrite($stream, $frame, $len);

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

    public function recv(&$buf){

        $len = strlen($buf);

        if(!strlen($buf) > 0)
            return false;

        $desc = is_resource($this->stream) ? "HOST=$this->address PORT=$this->port" : "PIPE";

        $this->log->write(W_DEBUG, "CONNECTION<-SOCKET: BYTES=$len $desc", $this->name);

        //Record this time as the last time we received data from the client
        $this->lastContact = time();

        if($this->status === WARLOCK_CONN_HANDSHAKE){

            if($this->node instanceof Node){

                if(!$this->completeHandshake($buf))
                    return false;

            }

        }elseif($this->status !== WARLOCK_CONN_ONLINE){

            if(!$this->processHandshake($buf))
                return false;

            return true;

        }

        /**
         * Sometimes we can get multiple frames in a single buffer so we cycle through
         * them until they are all processed.  This will even allow partial frames to be
         * added to the client frame buffer.
         */
        while($packet = $this->processFrame($buf)) {

            $this->log->write(W_DECODE, "CONNECTION<-PACKET: " . $packet, $this->name);

            if(!Master::$cluster->processPacket($this->node, $packet))
                $this->log->write(W_ERR, 'Negative response returned while processing packet!', $this->name);

        }

        return true;

    }


    /**
     * Processes a client data frame.
     *
     * @param mixed $frameBuffer
     *
     * @return mixed
     */
    public function processFrame(&$frameBuffer) {

        if($this->direct === true){

            if($this->payloadBuffer){

                $frameBuffer = $this->payloadBuffer .= $frameBuffer;

                $this->payloadBuffer = NULL;

                return $this->processFrame($frameBuffer);

            }

            if(($pos = strpos($frameBuffer, "\n")) === false){

                $this->payloadBuffer .= $frameBuffer;

                return false;

            }

            $payload = substr($frameBuffer, 0, $pos++);

            $frameBuffer = substr($frameBuffer, $pos);
            return $payload;

        }

        if ($this->frameBuffer) {

            $frameBuffer = $this->frameBuffer . $frameBuffer;

            $this->frameBuffer = NULL;

            return $this->processFrame($frameBuffer);

        }

        if (!$frameBuffer)
            return false;

        $this->log->write(W_DECODE2, "CONNECTION<-FRAME: " . implode(' ', $this->hexString($frameBuffer)), $this->name);

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

        $this->log->write(W_DEBUG, "CONNECTION<-OPCODE: $opcode", $this->name);

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
                    $this->log(W_WARN, 'Got final continuation frame but there is no payload in the buffer!?');

                $payload = $this->payloadBuffer . $payload;

                $this->payloadBuffer = '';

                break;

            case 1 : //Text frame
            case 2 : //Binary frame

                //These are our normal frame types which will already be processed into $payload.

                break;

            case 8 : //Close frame

                if($this->status === WARLOCK_CONN_ONLINE){

                    $this->log->write(W_DEBUG, "WEBSOCKET<-CLOSE: HOST=$this->address PORT=$this->port", $this->name);

                    $this->status = WARLOCK_CONN_CLOSING;

                    $frame = $this->frame('', 'close', false);

                    @fwrite($this->stream, $frame, strlen($frame));

                    if($this->disconnect(true))
                        $this->log->write(W_NOTICE, "Websockets connection closed to $this->address:$this->port", $this->name);

                }

                return false;

            case 9 : //Ping

                $this->log->write(W_DEBUG, "WEBSOCKET<-PING: HOST=$this->address PORT=$this->port", $this->name);

                $frame = $this->frame('', 'pong', false);

                @fwrite($this->stream, $frame, strlen($frame));

                return false;

            case 10 : //Pong

                $this->log->write(W_DEBUG, "WEBSOCKET<-PONG: HOST=$this->address PORT=$this->port", $this->name);

                $this->pong($payload);

                return false;

            default : //Unknown!

                $this->log->write(W_ERR, "Bad opcode received on Websocket connection from $this->address:$this->port", $this->name);

                $this->disconnect();

                return false;

        }

        $this->lastContact = time();

        return $payload;

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

    public function getStatus(){

        $warlock_conn_constants = array(
            'WARLOCK_CONN_OFFLINE',
            'WARLOCK_CONN_INIT',
            'WARLOCK_CONN_HANDSHAKE',
            'WARLOCK_CONN_ONLINE',
            'WARLOCK_CONN_CLOSING',
        );

        return $warlock_conn_constants[$this->status];

    }
}
