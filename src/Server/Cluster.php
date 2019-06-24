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

    function __construct(\Hazaar\Map $config){

        $this->log = Master::$instance->log;

        $this->config = $config;

        $this->name = ake($this->config->cluster, 'name', gethostname());

        $this->signal = new Signal($this->config->signal);

        if(($peers = $this->config->cluster['peers']) && $peers->count() > 0){

            foreach($peers as $peer){

                if(ake($peer, 'enabled', true) !== true)
                    continue;

                if($peer->has('host')){

                    if(!$peer->has('access_key'))
                        $peer->access_key = Master::$instance->config->admin['key'];

                    $this->peers[] = new Node\Peer(null, $peer->toArray());

                }else{

                    $this->log(W_ERR, 'Remote peers require a host address.');

                }
            }

        }

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

        if(($count = count($this->peers)) > 0){

            $this->log->write(W_INFO, "Found $count peers.");

            $this->process();

        }

        return true;

    }

    public function stop(){

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
    public function createNode(Connection $conn, $request){

        $type = ake($request, 'x-warlock-type', 'client');

        $this->log->write(W_DEBUG, "CLUSTER->ADDNODE: HOST=$conn->address PORT=$conn->port TYPE=$type", $this->name);

        if($type === 'client'){

            $node = new Node\Client($conn);

            $this->clients[$node->id] = $node;

        }elseif($type === 'peer'){

            die('SOCKET PEERS NOT DONE YET!');

            //$this->node = new Node\Peer($results['url']['CID']);

        }else{

            $this->log->write(W_ERR, 'Unknown client type requested: ' . $type);

            return false;

        }

        return $node;

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

        //If there is no frame ID, add on now.  This happens when the frame comes from a CLIENT and has not yet entered the network.
        if(!property_exists($frame, 'FID'))
            $packet = Master::$protocol->encode($type, $payload, array('FID' => uniqid()));

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
            if($type !== 'SUBSCRIBE'){

                foreach($this->peers as $peer)
                    $peer->send($packet);

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

        switch($type){

            case 'SUBSCRIBE':

                $filter = (property_exists($payload, 'filter') ? $payload->filter : NULL);

                return $this->signal->subscribe($node, $payload->id, $filter);

            case 'UNSUBSCRIBE' :

                return $this->signal->unsubscribe($node, $payload->id);

            case 'TRIGGER' :

                return $this->signal->trigger($node, $payload->id, ake($payload, 'data'), ake($payload, 'echo', false));

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

                if(!$peer instanceof Node\Peer)
                    continue;

                if($peer->conn->connected() !== true){

                    if($peer->connect() === false)
                        continue;

                    $this->log->write(W_DEBUG, "PEER->CONNECT: HOST={$peer->conn->address} PORT={$peer->conn->port}", $peer->name);

                    Master::$instance->addConnection($peer->conn);

                }

            }

        }

        $this->signal->queueCleanup();


        return;

    }

    private function checkClients(){

        if(!($this->config->client->check > 0 && is_array($this->clients) && count($this->clients) > 0))
            return;

        //Only ping if we havn't received data from the client for the configured number of seconds (default to 60).
        $when = time() - $this->config->client->check;

        foreach($this->clients as $client){

            if(!$client instanceof Node)
                continue;

            if($client->lastContact <= $when)
                $client->ping();

        }

        return;

    }

}
