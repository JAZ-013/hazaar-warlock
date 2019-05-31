<?php

namespace Hazaar\Warlock\Server;

class Cluster  {

    public $name;

    private $log;

    /**
     * Array of other warlocks to connect and share data with
     *
     * @var array Array of Hazaar\Warlock\Server\Peer object.
     */
    public $peers = array();

    public $peer_lookup = array();

    private $last_check = 0;

    function __construct(\Hazaar\Map $config){

        $this->log = Master::$instance->log;

        $this->name = $config->name;

        if(($peers = $config['peers']) && $peers->count() > 0){

            foreach($peers as $peer){

                if(ake($peer, 'enabled', true) !== true)
                    continue;

                if($peer->has('host')){

                    if(!$peer->has('access_key'))
                        $peer->access_key = Master::$instance->config->admin['key'];

                    $this->peers[] = new Peer($peer->toArray(), Master::$protocol);

                }else{

                    $this->log(W_ERR, 'Remote peers require a host address.');

                }
            }

        }

    }

    public function start(){

        if(($count = count($this->peers)) === 0)
            return false;

        $this->log->write(W_INFO, "Starting Warlock Cluster.  Found $count peers.");

        return $this->checkPeers();

    }

    public function addPeer(Client $peer){

        $this->log->write(W_NOTICE, "Link from peer $peer->id is now online at $peer->address:$peer->port", $peer->name);

        $socket_id = intval($peer->socket);

        $this->peers[] = $peer;

        $this->peer_lookup[$socket_id] = $peer;

        return array('peer' => $this->name);

    }

    public function checkPeers(){

        if(count($this->peers) === 0)
            return false;

        foreach($this->peers as $peer){

            if(!$peer instanceof Peer)
                continue;

            if(!$peer->connected()){

                if(($socket = $peer->connect()) === false)
                    continue;

                $this->log->write(W_DEBUG, "PEER->CONNECT: HOST=$peer->address PORT=$peer->port", $peer->name);

                $socket_id = intval($socket);

                Master::$instance->streams[$socket_id] = $socket;

                $this->peer_lookup[$socket_id] = $peer;

                continue;

            }

        }

        return true;

    }

    private function sendAll($command, $payload = null){

        $this->log->write(W_DEBUG, "CLUSTER->SEND: $command");

        $frame_id = uniqid();

        foreach($this->peers as $peer)
            $peer->send($command, $payload, $frame_id);

        return true;

    }

    public function sendEvent($event_id, $trigger_id, $data) {

        $this->log->write(W_DEBUG, "CLUSTER->EVENT: NAME=$event_id TRIGGER_ID=$trigger_id");

        $packet = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'time' => microtime(true),
            'data' => $data
        );

        return $this->sendAll('EVENT', $packet);

    }

}
