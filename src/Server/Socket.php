<?php

namespace Hazaar\Warlock\Server;

abstract class Socket {

    private $log;

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

    public $admin = FALSE;

    /**
     * This is an array of event_id and socket pairs
     * @var array
     */
    public $subscriptions = array();

    /**
     * If the client has an associated process.  ie: a service
     * @var array The proccess array
     */
    public $process;

    /**
     * If the client has any child jobs
     * @var mixed
     */
    public $jobs = array();

    function __construct(&$server, $id, $type = 'client', $resource = NULL, $uid = NULL, $options = array()) {

        $this->log = new \Hazaar\Warlock\Server\Logger();

        $allowed_types = array('client', 'service', 'admin');

        if(!in_array($type, $allowed_types))
            $type = 'client';

        $this->id = $id;

        $this->username = base64_decode($uid);

        $this->since = time();

        $this->type = $type;

        $this->resource = $resource;

        //if ($this->username != NULL)
        //    stdout(W_NOTICE, "USER: $this->username");

        if (is_resource($this->resource)) {

            $resource_type = get_resource_type($this->resource);

            if ($resource_type == 'Socket')
                socket_getpeername($this->resource, $this->address, $this->port);

            $this->log->write(W_NOTICE, "ADD: TYPE=$resource_type CLIENT=$this->id SOCKET=$this->resource");

            $this->lastContact = time();

        }

        $this->server = $server;

        $this->ping['wait'] = ake($options, 'wait', 15);

        $this->ping['pings'] = ake($options, 'pings', 5);

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

                $this->log->write(W_WARN, 'Subscription failed.  Bad socket resource!');

                return FALSE;
            }

            $resource = $this->resource;
        }

        $this->subscriptions[$event_id] = $resource;

        $this->log->write(W_NOTICE, "SUBSCRIBE: EVENT=$event_id CLIENT=$this->id COUNT=" . count($this->subscriptions));

        return TRUE;

    }

    public function unsubscribe($event_id, $resource = NULL) {

        if (!array_key_exists($event_id, $this->subscriptions))
            return FALSE;

        if ($resource && !is_resource($resource))
            return FALSE;

        if ($resource && $resource != $this->subscriptions[$event_id])
            return FALSE;

        unset($this->subscriptions[$event_id]);

        $this->log->write(W_DEBUG, "UNSUBSCRIBE: EVENT=$event_id CLIENT=$this->id COUNT=" . count($this->subscriptions));

        return TRUE;

    }

    public function sendEvent($event_id, $trigger_id, $data) {

        if (!array_key_exists($event_id, $this->subscriptions)) {

            $this->log->write(W_WARN, "Client $this->id is not listening for $event_id");

            return FALSE;

        }

        $resource = $this->subscriptions[$event_id];

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(TRUE),
            'data' => $data
        );

        $result = $this->server->send($resource, 'EVENT', $packet, $this->isLegacy());

        // Disconnect if we are a socket but not a websocket (legacy connection) and the result was negative.
        if (get_resource_type($resource) == 'Socket' && $this->isLegacy() && $result)
            $this->server->disconnect($resource);

        return TRUE;

    }

    public function ping(){

        if((time() - $this->ping['wait']) < $this->ping['last'])
            return false;

        $this->ping['attempts']++;

        if($this->ping['attempts'] > $this->ping['pings']){

            $this->log->write(W_WARN, 'Disconnecting client due to lack of PONG!');

            $this->server->disconnect($this->resource);

            return false;

        }

        $this->ping['last'] = time();

        $this->log->write(W_DEBUG, 'CLIENT_PING: ATTEMPTS=' . $this->ping['attempts'] . ' LAST=' . date('c', $this->ping['last']));

        return $this->server->ping($this->resource);

    }

    public function pong(){

        $this->ping['attempts'] = 0;

        $this->ping['last'] = 0;

    }

}
