<?php

namespace Hazaar\Warlock\Server;

define('STREAM_MAX_RECV_LEN', 65535);

class Cluster  {

    private $config;

    public $name;

    private $log;

    /**
     * Array of other warlocks to connect and share data with
     *
     * @var array Array of Hazaar\Warlock\Server\Peer object.
     */
    public $peers = array();

    /**
     * Array of normal clients that are currently connected
     * @var array
     */
    public $clients = array();

    private $last_check = 0;

    private $frames = array();

    public $stats = array(
        'clients' => 0,         // Total number of connected clients
        'peers' => 0,
        'processed' => 0,       // Total number of processed jobs & events
        'execs' => 0,           // The number of successful job executions
        'lateExecs' => 0,       // The number of delayed executions
        'failed' => 0,          // The number of failed job executions
        'processes' => 0,       // The number of currently running processes
        'retries' => 0,         // The total number of job retries
        'queue' => 0,           // Current number of jobs in the queue
        'limitHits' => 0       // The number of hits on the process limiter
    );

    private $kv_store;

    function __construct(\Hazaar\Map $config){

        $this->log = Master::$instance->log;

        $this->config = $config;

        $this->name = ake($this->config, 'name', gethostname());

        $this->signal = new Signal(Master::$instance->config['signal']);

        $this->runner = new Runner(Master::$instance->config['runner']);

    }

    function __destruct() {

    }

    public function start(){

        $this->log->write(W_INFO, "Starting Warlock cluster manager.");

        if($this->config->has('subscribe')){

            $this->log->write(W_NOTICE, 'Found ' . $this->config->subscribe->count() . ' global events');

            foreach($this->config->subscribe as $event_name => $event_func){

                if(!($callable = $this->callable($event_func))){

                    $this->log->write(W_ERR, 'Global event config contains invalid callable for event: ' . $event_name);

                    continue;

                }

                $this->signal->subscribeCallable($event_name, $callable);

            }

        }

        if($this->config->has('schedule')){

            $this->log->write(W_NOTICE, 'Scheduling ' . $this->config->schedule->count() . ' jobs');

            foreach($this->config->schedule as $job){

                if(!$job->has('exec'))
                    continue;

                $application = (object)array(
                    'path' => APPLICATION_PATH,
                    'env'  => APPLICATION_ENV
                );

                if(!($callable = $this->callable(ake($job, 'exec')))){

                    $this->log->write(W_ERR, 'Warlock schedule config contains invalid callable.');

                    continue;

                }

                $exec = (object)array('callable' => $callable);

                if($args = ake($job, 'args'))
                    $exec->params = $args->toArray();

                Master::$instance->scheduleJob(ake($job, 'when'), $exec, $application, ake($job, 'tag'), ake($job, 'overwrite'));

            }

        }

        if(($peers = $this->config['peers']) && $peers->count() > 0){

            foreach($peers as $peer_item){

                if(ake($peer_item, 'enabled', true) !== true)
                    continue;

                if(!$peer_item->has('host')){

                    $this->log(W_ERR, 'Remote peers require a host address.');

                    continue;

                }

                $target = $peer_item->get('host') . ':' . $peer_item->get('port');

                $id = hash('crc32b', $target);

                if(array_key_exists($id, $this->peers)){

                    $this->log->write(W_WARN, 'Duplicate peer connection to ' . $target);

                    continue;

                }

                if(!$peer_item->has('access_key'))
                    $peer_item->access_key = $this->config['access_key'];

                if(!$peer_item->has('timeout'))
                    $peer_item->timeout = $this->config['connect_timeout'];

                $peer = new Node\Peer(null, $peer_item->toArray());

                $peer->name = Master::$instance->config->cluster['name'];

                $this->peers[$id] = $peer;

            }

            if(($count = count($this->peers)) > 0){

                $this->log->write(W_INFO, "Found $count peers.");

                $this->process();

            }

        }

        return $this->runner->start();

    }

    public function stop(){

        $this->log->write(W_NOTICE, 'Shutting down cluster.', $this->name);

        $this->runner->stop();

    }

    /**
     * Process a handshake request from an unknown connection
     *
     * This is the main WebSocket connection initiator.  If data is received on a stream and we don't have a Node
     * object on that stream, this method is called to attempt to initiate a websocket session.
     *
     * @param mixed $socket
     * @param mixed $request
     * @return boolean
     */
    public function createNode(Connection $conn, $request = null){

        $type = ake($request, 'x-warlock-client-type', 'client');

        $this->log->write(W_DEBUG, "CLUSTER->ADDNODE: HOST=$conn->address PORT=$conn->port TYPE=$type", $this->name);

        if($type === 'client'){

            $node = new Node\Client($conn);

            $this->clients[$node->id] = $node;

        }elseif($type === 'peer'){

            if(!(($access_key = ake($request, 'x-warlock-access-key')) && ($name = ake($request, 'x-warlock-peer-name'))))
                return false;

            if(array_key_exists($name, $this->peers)){

                $this->log->write(W_WARN, "A peer with name '$name' already connected!");

                return false;

            }

            $node = new Node\Peer($conn, $this->config->toArray());

            $node->name = $this->name;

            if(!$node->auth($name, $access_key)){

                $this->log->write(W_WARN, 'Peer connected with incorrect access key!');

                return false;

            }

            $this->peers[$node->id] = $node;

        }else{

            $this->log->write(W_ERR, 'Unknown client type requested: ' . $type);

            return false;

        }

        return $node;

    }

    public function addNode(Node $node){

        if(!Master::$instance->addConnection($node->conn))
            return false;

        if($node instanceof Node\Peer)
            $this->peers[$node->id] = $node;
        else
            $this->clients[$node->id] = $node;

        return true;

    }

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
    public function removeNode(Node $node) {

        if($node instanceof Node\Client){

            if(array_key_exists($node->id, $this->clients))
                unset($this->clients[$node->id]);
            else return false;

            $this->signal->disconnect($node);

            $this->log->write(W_DEBUG, "CLUSTER->REMOVENODE: CLIENT=$node->id", $this->name);

            $this->stats['clients']--;

        }elseif($node instanceof Node\Peer){

            if(array_key_exists($node->id, $this->peers))
                unset($this->peers[$node->id]);
            else return false;

            $this->log->write(W_DEBUG, "CLUSTER->REMOVENODE: PEER=$node->id", $this->name);

            $this->stats['peers']--;

        }

        return true;

    }

    public function processPacket(Node $node, $packet){

        $payload = null;

        $frame = null;

        if(!($type = Master::$protocol->decode($packet, $payload, $frame))){

            $reason = Master::$protocol->getLastError();

            $this->log->write(W_ERR, "Protocol error: $reason", $this->name);

            $node->conn->disconnect();

            return false;

        }

        //If there is no frame ID, add one now.  This happens when the frame comes from a CLIENT and has not yet entered the network.
        if(property_exists($frame, 'FID')){

            $frame_id = $frame->FID;

            //If we have seen this frame, then silently ignore it.
            if(array_key_exists($frame->FID, $this->frames))
                return true;

        }else{

            $frame_id = uniqid();

            $packet = Master::$protocol->encode($type, $payload, array('FID' => $frame_id));

        }

        $this->frames[$frame_id] = array(
            'expires' => time() + $this->config['frame_lifetime'],
            'peers' => array($node->id => time())
        );

        if(property_exists($frame, 'TME'))
            $this->offset = (time() - $frame->TME);

        try{

            /**
             * Forward any non-subscribe events to all connected peers.  This is the whole "mesh-network" bit.
             *
             * Notes:
             * * Frames with frame_ids will eventually be ignored and recorded.
             * * I may need to come up with a better frame forwarding scheme.  Perhaps based on frame IDs or something.
             */
            if($type === 'TRIGGER'){

                foreach($this->peers as $peer){

                    if(array_key_exists($peer->id, $this->frames[$frame_id]['peers']) || $peer->online() !== true)
                        continue;

                    if($peer->conn->send($packet))
                        $this->frames[$frame_id]['peers'][$peer->id] = time();

                }

            }

            if (!$this->processCommand($node, $type, $payload))
                throw new \Exception('Negative response returned while processing command!');

        }
        catch(\Exception $e){

            $this->log->write(W_ERR, 'An error occurred processing the command: ' . $type, $this->name);

            $node->send('error', array(
                'reason' => $e->getMessage(),
                'command' => $type
            ));

        }

        return true;

    }

    private function processCommand(Node $node, $type, $payload){

        $this->log->write(W_DEBUG, $node->type . "<-$type: CLIENT=$node->id", $node->name);

        if($this->kv_store !== NULL && substr($type, 0, 2) === 'KV')
            return $this->kv_store->process($node, $type, $payload);

        switch($type){

            case 'SUBSCRIBE':

                $filter = (property_exists($payload, 'filter') ? $payload->filter : NULL);

                return $this->signal->subscribe($node, $payload->id, $filter);

            case 'UNSUBSCRIBE' :

                return $this->signal->unsubscribe($node, $payload->id);

            case 'TRIGGER' :

                return $this->signal->trigger($node, $payload->id, ake($payload, 'data'), ake($payload, 'echo', false));

            case 'LOG':

                $this->log->write(ake($payload, 'level', W_INFO), ake($payload, 'msg'));

                return true;

            case 'STATUS':

                $node->status = $payload;

                return true;

            default:

                return $node->processCommand($type, $payload);

        }

        return false;

    }

    public function process(){

        if(($check = Master::$instance->config->client->check) > 0 && is_array($this->clients) && count($this->clients) > 0){

            //Only ping if we havn't received data from the client for the configured number of seconds (default to 60).
            $when = time() - $check;

            foreach($this->clients as $client){

                if(!$client instanceof Node)
                    continue;

                if($client->conn->lastContact <= $when)
                    $client->conn->ping();

            }

        }

        if(count($this->peers) > 0){

            foreach($this->peers as $peer){

                //If the peer is already ONLINE, there's nothing to do
                if(!$peer instanceof Node\Peer)
                    continue;

                while($peer->ping() !== true);

            }

        }

        if(count($this->frames) > 0){

            $now = time();

            foreach($this->frames as $id => $frame){

                if(!($now >= $frame['expires']))
                    continue;

                unset($this->frames[$id]);

            }

        }

        $this->runner->process();

        $this->signal->queueCleanup();

        if($this->kv_store)
            $this->kv_store->expireKeys();

        return;

    }

    private function checkClients(){

        $check = Master::$instance->config->client['check'];

        if(!($check > 0 && is_array($this->clients) && count($this->clients) > 0))
            return;

        //Only ping if we havn't received data from the client for the configured number of seconds (default to 60).
        $when = time() - $check;

        foreach($this->clients as $client){

            if(!$client instanceof Node)
                continue;

            if($client->lastContact <= $when)
                $client->ping();

        }

        return;

    }

    public function startKV(){

        $this->log->write(W_NOTICE, 'Initialising KV Store');

        $this->kv_store = new Kvstore($this->config->kvstore['persist'], $this->config->kvstore['compact']);

        return true;

    }

}
