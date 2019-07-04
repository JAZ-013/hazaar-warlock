<?php

namespace Hazaar\Warlock\Server;

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

    public $admins = array();

    public $next_announce = 0;

    public $cluster_peers = array();

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

        if($this->config->kvstore['enabled'] === true)
            $this->kv_store = new Kvstore($this->config->kvstore);

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

        if($node instanceof Node\Peer){

            if(array_key_exists($node->id, $this->peers))
                unset($this->peers[$node->id]);
            else return false;

            $this->log->write(W_DEBUG, "CLUSTER->REMOVENODE: PEER=$node->id", $this->name);

            $this->stats['peers']--;

        }else{

            if(array_key_exists($node->id, $this->clients))
                unset($this->clients[$node->id]);
            else return false;

            if(array_key_exists($node->id, $this->admins))
                unset($this->admins[$node->id]);

            $this->signal->disconnect($node);

            $this->log->write(W_DEBUG, "CLUSTER->REMOVENODE: CLIENT=$node->id", $this->name);

            $this->stats['clients']--;

            if($node instanceof Node\Client && ($count = count($node->jobs)) > 0){

                $this->log->write(W_NOTICE, 'Disconnected WebSocket client has ' . $count . ' running/pending child jobs', $node->name);

                foreach($node->jobs as $job){

                    if($job->detach !== true)
                        $job->status = STATUS_CANCELLED;

                }

            }

        }

        return true;

    }

    /**
     * Forward any frames to all connected peers.  This is the whole "mesh-network" bit.
     *
     * Notes:
     * * Frames with frame_ids will eventually be ignored and recorded.
     * * I may need to come up with a better frame forwarding scheme.  Perhaps based on type IDs or something.
     *
     * @param mixed $frame
     * @param Node $sourceNode
     * @return boolean
     */
    private function forwardFrame(\stdClass $frame, Node $sourceNode = null){

        if(!property_exists($frame, 'FID'))
            $frame->FID = uniqid();

        $frame_id = $frame->FID;

        $this->frames[$frame_id] = array(
            'expires' => time() + $this->config['frame_lifetime'],
            'peers' => array()
        );

        if($sourceNode)
            $this->frames[$frame_id]['peers'][$sourceNode->id] = time();

        $packet = json_encode($frame);

        foreach($this->peers as $peer){

            if((array_key_exists($frame_id, $this->frames) && array_key_exists($peer->id, $this->frames[$frame_id]['peers'])) || $peer->online() !== true)
                continue;

            if($peer->conn->send($packet))
                $this->frames[$frame_id]['peers'][$peer->id] = time();

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

        //If we have seen this frame, then silently ignore it.
        if(property_exists($frame, 'FID') && array_key_exists($frame->FID, $this->frames))
            return true;

        if(property_exists($frame, 'TME'))
            $this->offset = (time() - $frame->TME);

        try{

            if($frame->TYP === 0x12 || $frame->TYP === 0xA1)
                $this->forwardFrame($frame, $node);

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

        $type_id = Master::$protocol->getType($type);

        $this->log->write(W_DEBUG, $node->type . "<-$type: ID=0x"
            . strtoupper(str_pad(dechex($type_id), 2, STR_PAD_LEFT))
            . " CLIENT=$node->id", $node->name);

        if($type_id < 0x20){

            switch($type){

                case 'AUTH':

                    return $this->authorise($node, $payload);

                case 'OK':

                    //No action
                    return true;

                case 'ERROR':

                    $this->log->write(W_ERR, $payload, $node->name);

                    return true;

                case 'LOG':

                    return $this->commandLog($node, $payload);

                case 'DEBUG':

                    $this->log->write(W_DEBUG, ake($payload, 'data'), $node->name);

                    return true;

                case 'SUBSCRIBE':

                    return $this->signal->subscribe($node, $payload->id, ake($payload, 'filter'));

                case 'UNSUBSCRIBE' :

                    return $this->signal->unsubscribe($node, $payload->id);

                case 'TRIGGER' :

                    return $this->signal->trigger($node, $payload->id, ake($payload, 'data'), ake($payload, 'echo', false));

            }

            return $node->processCommand($type, $payload);

        }

        if($node instanceof Node\Client && !array_key_exists($node->id, $this->admins))
            return false;

        if($type_id >= 0x20 && $type_id <= 0x29){

            return $this->runner->processCommand($node, $type, $payload);

        }elseif($type_id >= 0x40 && $type_id <= 0x4F){

            if(!$this->kv_store instanceof Kvstore){

                $this->log->write(W_WARN, 'KV Storage is currently disabled!');

                return false;

            }

            return $this->kv_store->processCommand($node, $type, $payload);

        }

        switch($type){

            case 'ANNOUNCE':

                if(!property_exists($payload, 'name'))
                    return false;

                if(!array_key_exists($payload->name, $this->cluster_peers)){

                    $this->log->write(W_INFO, "Peer '$payload->name' is now online!", $this->name);

                    $this->next_announce = 0;

                }

                $this->cluster_peers[$payload->name] = array(
                    'start' => $payload->start,
                    'load' => $payload->load,
                    'last' => time(),
                    'status' => 'ONLINE'
                );

                return true;

            case 'STATUS':

                $node->send('STATUS', $this->getStatus());

                return true;

            case 'SHUTDOWN':

                return $this->commandShutdown($node, $payload);

        }

        return false;

    }

    private function commandLog(Node $node, \stdClass $payload){

        if(!property_exists($payload, 'msg'))
            throw new \Exception('Unable to write to log without a log message!');

        $level = ake($payload, 'level', W_INFO);

        $name = ake($payload, 'name', $node->name);

        if(is_array($payload->msg)){

            foreach($payload->msg as $msg)
                $this->commandLog($node, (object)array('level' => $level, 'msg' => $msg, 'name' => $name));

        }else{

            $this->log->write($level, ake($payload, 'msg', '--'), $name);

        }

        return true;

    }

    private function commandShutdown(Node $node, $payload){

        if(!array_key_exists($node->id, $this->admins))
            return false;

        $delay = ake($payload, 'delay', 0);

        $this->log->write(W_NOTICE, "Shutdown requested (Delay: $delay)");

        if(!Master::$instance->shutdown($delay))
            return false;

        $node->send('OK', array('command' => 'SHUTDOWN'));

        return true;

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

                //If the peer is already ONLINE, there's nothing to do
                while($peer->ping() !== true);

            }

        }

        if($this->next_announce <= time()){

            $expire = time() - $this->config['peer_expire'];

            foreach($this->cluster_peers as $name => $peer){

                if($peer['status'] === 'ONLINE' && $peer['last'] >= $expire)
                    continue;

                $this->log->write(W_INFO, "Peer '$name' is now offline!", $this->name);

                $this->cluster_peers[$name]['status'] = 'OFFLINE';

            }

            Master::$protocol->encode('announce', array('name' => $this->name, 'start' => Master::$instance->start, 'load' => sys_getloadavg()), $frame);

            $this->forwardFrame($frame);

            $this->next_announce = time() + $this->config['announce'];

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

    /**
     * Returns the current server status
     *
     * @param mixed $full
     * @return array
     */
    private function getStatus($full = true) {

        $status = array(
            'state' => 'running',
            'pid' => getmypid(),
            'started' => Master::$instance->start,
            'uptime' => time() - Master::$instance->start,
            'memory' => memory_get_usage(),
            'stats' => $this->stats,
            'peers' => $this->cluster_peers
        );

        return $status;

    }


    public function authorise(Node $node, $payload){

        if(!($payload instanceof \stdClass
            && property_exists($payload, 'access_key')
            && $payload->access_key === Master::$instance->config->client->admin['key']
        )) return false;

        $this->log->write(W_NOTICE, 'Warlock control authorised to ' . $node->id, $node->name);

        $this->admins[$node->id] = $node;

        $node->send('OK');

        return true;

    }

}
