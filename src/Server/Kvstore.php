<?php

namespace Hazaar\Warlock\Server;

class Kvstore {

    private $log;

    private $kv_store = array();

    private $kv_expire = array();

    private $db;

    private $compact_time = 0;

    private $last_compact = 0;

    function __construct($persistent = false, $compact_time = null) {

        $this->log = Master::$instance->log;

        if($persistent === true){

            $db_file = new \Hazaar\File(\Hazaar\Warlock\Server\Master::$instance->runtimePath('kvstore.db'));

            $this->db = new \Hazaar\Btree($db_file);

            if($compact_time > 0){

                $this->log->write(W_NOTICE, 'KV Store persistent storage compaction enabled');

                $this->compact_time = $compact_time;

                $this->last_compact = $db_file->ctime();

            }

        }

    }

    public function expireKeys(){

        $now = time();

        foreach($this->kv_expire as $namespace => &$slots){

            ksort($this->kv_expire[$namespace], SORT_NUMERIC);

            foreach($slots as $time => &$keys){

                if($time > $now)
                    break;

                foreach($keys as $key){

                    $this->log->write(W_DEBUG, 'KVEXPIRE: ' . $namespace . '::' . $key);

                    unset($this->kv_store[$namespace][$key]);

                }

                unset($this->kv_expire[$namespace][$time]);

            }

            if(count($slots) === 0)
                unset($this->kv_expire[$namespace]);

        }

        if($this->db !== null
            && $this->compact_time > 0
            && $this->last_compact + $this->compact_time <= ($now = time())){

            $this->log->write(W_INFO, 'Compacting KV Persistent Storage');

            $this->db->compact();

            $this->last_compact = $now;

        }

    }

    public function & touch($namespace, $key){

        if(!(array_key_exists($namespace, $this->kv_store) && array_key_exists($key, $this->kv_store[$namespace]))){

            if($this->db
                && ($slot = $this->db->get($key))
                && (array_key_exists('e', $slot) && $slot['e'] > time())){

                $this->kv_store[$namespace][$key] =& $slot;

            }else{

                $this->kv_store[$namespace][$key] = array('v' => null);

                return $this->kv_store[$namespace][$key];

            }

        }else $slot =& $this->kv_store[$namespace][$key];

        if(array_key_exists('e', $slot)
            && array_key_exists($namespace, $this->kv_expire)
            && array_key_exists($slot['e'], $this->kv_expire[$namespace])
            && ($index = array_search($key, $this->kv_expire[$namespace][$slot['e']])) !== false)
            unset($this->kv_expire[$namespace][$slot['e']][$index]);

        if(array_key_exists('t', $slot)){

            $slot['e'] = time() + $slot['t'];

            $this->kv_expire[$namespace][$slot['e']][] = $key;

            $this->db->set($key, $slot);

        }

        return $slot;

    }

    public function process(Node $client, $command, &$payload){

        if(!$payload)
            return false;

        $namespace = (property_exists($payload, 'n') ? $payload->n : 'default');

        $this->log->write(W_DEBUG, $command . ': ' . $namespace . (property_exists($payload, 'k') ? '::' . $payload->k : ''));

        switch($command){

            case 'KVGET':

                return $this->get($client, $payload, $namespace);

            case 'KVSET':

                return $this->set($client, $payload, $namespace);

            case 'KVHAS':

                return $this->has($client, $payload, $namespace);

            case 'KVDEL':

                return $this->del($client, $payload, $namespace);

            case 'KVLIST':

                return $this->list($client, $payload, $namespace);

            case 'KVCLEAR':

                return $this->clear($client, $payload, $namespace);

            case 'KVPULL':

                return $this->pull($client, $payload, $namespace);

            case 'KVPUSH':

                return $this->push($client, $payload, $namespace);

            case 'KVPOP':

                return $this->pop($client, $payload, $namespace);

            case 'KVSHIFT':

                return $this->shift($client, $payload, $namespace);

            case 'KVUNSHIFT':

                return $this->unshift($client, $payload, $namespace);

            case 'KVCOUNT':

                return $this->count($client, $payload, $namespace);

            case 'KVINCR':

                return $this->incr($client, $payload, $namespace);

            case 'KVDECR':

                return $this->decr($client, $payload, $namespace);

            case 'KVKEYS':

                return $this->keys($client, $payload, $namespace);

            case 'KVVALS':

                return $this->values($client, $payload, $namespace);

        }

        return null;

    }

    public function get(Node $client, $payload, $namespace){

        $value = null;

        if(property_exists($payload, 'k')){

            $slot = $this->touch($namespace, $payload->k);

            $value = $slot['v'];

        }else{

            $this->log->write(W_ERR, 'KVGET requires \'k\'');

        }

        return $client->send('KVGET', $value);

    }

    public function set(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            if(array_key_exists($namespace, $this->kv_store)
                && array_key_exists($payload->k, $this->kv_store[$namespace])
                && array_key_exists('e', $this->kv_store[$namespace][$payload->k])){

                $e = $this->kv_store[$namespace][$payload->k]['e'];

                if(($key = array_search($payload->k, $this->kv_expire[$namespace][$e])) !== false)
                    unset($this->kv_expire[$namespace][$e][$key]);

            }

            $slot = array('v' => ake($payload, 'v'));

            if(property_exists($payload, 't')){

                $slot['t'] = $payload->t;

                $slot['e'] = time() + $payload->t;

                $this->kv_expire[$namespace][$slot['e']][] = $payload->k;

            }

            $this->kv_store[$namespace][$payload->k] = $slot;

            $result = true;

            if($this->db)
                $this->db->set($payload->k, $slot);

        }else{

            $this->log->write(W_ERR, 'KVSET requires \'k\'');

        }

        return $client->send('KVSET', $result);

    }

    public function has(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            if(!($result = (array_key_exists($namespace, $this->kv_store) && array_key_exists($payload->k, $this->kv_store[$namespace])))){

                if($this->db
                    && ($slot = $this->db->get($payload->k))
                    && (!array_key_exists('e', $slot) || $slot['e'] > time())){

                    $result = true;

                    $this->kv_store[$namespace][$payload->k] =& $slot;

                }

            }

        }else{

            $this->log->write(W_ERR, 'KVHAS requires \'k\'');

        }

        $client->send('KVHAS', $result);

        return true;

    }

    public function del(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            $result = (array_key_exists($namespace, $this->kv_store) && array_key_exists($payload->k, $this->kv_store[$namespace]));

            if($result === true){

                unset($this->kv_store[$namespace][$payload->k]);

                if($this->db)
                    $this->db->remove($payload->k);

            }

        }else{

            $this->log->write(W_ERR, 'KVDEL requires \'k\'');

        }

        return $client->send('KVDEL', $result);

    }

    public function list(Node $client, $payload, $namespace){

        $list = null;

        if(array_key_exists($namespace, $this->kv_store)){

            $list = array();

            if($this->db){

            }

            foreach($this->kv_store[$namespace] as $key => $data)
                $list[$key] = $data['v'];

        }

        return $client->send('KVLIST', $list);

    }

    public function clear(Node $client, $payload, $namespace){

        $this->kv_store[$namespace] = array();

        return $client->send('KVCLEAR', true);

    }

    public function pull(Node $client, $payload, $namespace){

        $result = null;

        if(property_exists($payload, 'k')){

            if(array_key_exists($namespace, $this->kv_store) && array_key_exists($payload->k, $this->kv_store[$namespace])){

                $result = $this->kv_store[$namespace][$payload->k]['v'];

                unset($this->kv_store[$namespace][$payload->k]);

            }

        }else{

            $this->log->write(W_ERR, 'KVPULL requires \'k\'');

        }

        return $client->send('KVPULL', $result);

    }

    public function push(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(is_array($slot['v']) && property_exists($payload, 'v'))
                $result = array_push($slot['v'], $payload->v);

        }else{

            $this->log->write(W_ERR, 'KVPUSH requires \'k\'');

        }

        return $client->send('KVPUSH', $result);

    }

    public function pop(Node $client, $payload, $namespace){

        $result = null;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(is_array($slot['v']))
                $result = array_pop($slot['v']);

        }else{

            $this->log->write(W_ERR, 'KVPOP requires \'k\'');

        }

        return $client->send('KVPOP', $result);

    }

    public function shift(Node $client, $payload, $namespace){

        $result = null;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(is_array($slot['v']))
                $result = array_shift($slot['v']);

        }else{

            $this->log->write(W_ERR, 'KVSHIFT requires \'k\'');

        }

        return $client->send('KVSHIFT', $result);

    }

    public function unshift(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(is_array($slot['v']) && property_exists($payload, 'v'))
                $result = array_unshift($slot['v'], $payload->v);

        }else{

            $this->log->write(W_ERR, 'KVUNSHIFT requires \'k\'');

        }

        return $client->send('KVUNSHIFT', $result);

    }

    public function count(Node $client, $payload, $namespace){

        $result = null;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(is_array($slot['v']))
                $result = count($slot['v']);

        }else{

            $this->log->write(W_ERR, 'KVCOUNT requires \'k\'');

        }

        return $client->send('KVCOUNT', $result);

    }

    public function incr(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(!is_int($slot['v']))
                $slot['v'] = 0;

            $result = ($slot['v'] += (property_exists($payload, 's') ? $payload->s : 1));

        }else{

            $this->log->write(W_ERR, 'KVINCR requires \'k\'');

        }

        return $client->send('KVINCR', $result);

    }

    public function decr(Node $client, $payload, $namespace){

        $result = false;

        if(property_exists($payload, 'k')){

            $slot =& $this->touch($namespace, $payload->k);

            if(!is_int($slot['v']))
                $slot['v'] = 0;

            $result = ($slot['v'] -= (property_exists($payload, 's') ? $payload->s : 1));

        }else{

            $this->log->write(W_ERR, 'KVDECR requires \'k\'');

        }

        return $client->send('KVDECR', $result);

    }

    public function keys(Node $client, $payload, $namespace){

        $result = null;

        if(array_key_exists($namespace, $this->kv_store))
            $result = array_keys($this->kv_store[$namespace]);

        return $client->send('KVKEYS', $result);

    }

    public function values(Node $client, $payload, $namespace){

        $result = null;

        if(array_key_exists($namespace, $this->kv_store))
            $result = array_values($this->kv_store[$namespace]);

        return $client->send('KVVALS', $result);

    }
}
