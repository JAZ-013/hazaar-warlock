<?php

namespace Hazaar\Warlock\Server;

class Service extends \Hazaar\Model\Strict {

    public function init(){

        return array(
            'name' => array(
                'type' => 'string'
            ),
            'enabled' => array(
                'type' => 'boolean',
                'default' => false
            ),
            'status' => array(
                'type' => 'integer',
                'default' => HAZAAR_SERVICE_INIT
            ),
            'job' => array(
                'type' => 'Job\Service'
            ),
            'restarts' => array(
                'type' => 'integer',
                'default' => 0
            ),
            'last_heartbeat' => array(
                'type' => 'integer',
                'default' => 0
            ),
            'heartbeats' => array(
                'type' => 'integer',
                'default' => 0
            ),
            'info' => array(
                'type' => 'array'
            )
        );

    }

    public function status() {

        switch ($this->status) {

            case HAZAAR_SERVICE_INIT:
                $ret = 'init';
                break;

            case HAZAAR_SERVICE_READY :
                $ret = 'ready';
                break;

            case HAZAAR_SERVICE_RUNNING :
                $ret = 'running';
                break;

            case HAZAAR_SERVICE_SLEEP :
                $ret = 'sleeping';
                break;

            case HAZAAR_SERVICE_STOPPED :
                $ret = 'stopped';
                break;

            case HAZAAR_SERVICE_STOPPING :
                $ret = 'stopping';
                break;

            case HAZAAR_SERVICE_ERROR :
                $ret = 'error';
                break;

            default :
                $ret = 'invalid';
                break;
        }

        return $ret;

    }

    public function disable($expire = null){

        $this->enabled = false;

        if ($this->job instanceof Job\Service)
            $this->job->cancel($expire);

        return true;
    }

}