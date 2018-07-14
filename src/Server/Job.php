<?php

namespace Hazaar\Warlock\Server;

/**
 * STATUS CONSTANTS
 */
define('STATUS_INIT', 0);

define('STATUS_QUEUED', 1);

define('STATUS_QUEUED_RETRY', 2);

define('STATUS_STARTING', 3);

define('STATUS_RUNNING', 4);

define('STATUS_COMPLETE', 5);

define('STATUS_CANCELLED', 6);

define('STATUS_ERROR', 7);

abstract class Job extends \Hazaar\Model\Strict {

    protected $log;

    public function init(){

        return array(
            'id' => 'string',
            'type' => 'string',
            'status' => array(
                'type' => 'int',
                'default' => STATUS_INIT,
                'update' => array(
                    'post' => function(){
                        $this->log->write(W_NOTICE, 'STATUS: ' . strtoupper($this->status()), $this->id);
                    }
                )
            ),
            'tag' => 'string',
            'info' => 'string',
            'access_key' => array(
                'type' => 'string',
                'value' => uniqid()
            ),
            'enabled' => array(
                'type' => 'boolean',
                'default' => true
            ),
            'start' => array(
                'type' => 'int',
                'default' => time()
            ),
            'application' => array(
                'type' => 'model',
                'items' => array(
                    'path' => array(
                        'type' => 'string',
                        'default' => APPLICATION_PATH
                    ),
                    'env' => array(
                        'type' => 'string',
                        'default' => APPLICATION_ENV
                    )
                )
            ),
            'retries' => array(
                'type' => 'int',
                'default' => 0
            ),
            'expire' => array(
                'type' => 'int',
                'default' => 0
            ),
            'respawn' => array(
                'type' => 'boolean',
                'default' => false
            ),
            'respawn_delay' => array(
                'type' => 'int',
                'default' => 0
            ),
            'restarts' => array(
                'type' => 'int',
                'default' => 0
            ),
            'params' => array(
                'type' => 'array'
            ),
            'process' => array(
                'type' => 'Hazaar\Warlock\Server\Process'
            ),
            'last_heartbeat' => array(
                'type' => 'int'
            ),
            'heartbeats' => array(
                'type' => 'int',
                'default' => 0
            )
        );

    }

    final public function construct(){

        $this->log = new Logger();

    }

    public function status() {

        switch ($this->status) {

            case STATUS_QUEUED :
                $ret = 'queued';
                break;

            case STATUS_QUEUED_RETRY :
                $ret = 'queued (retrying)';
                break;

            case STATUS_STARTING :
                $ret = 'starting';
                break;

            case STATUS_RUNNING :
                $ret = 'running';
                break;

            case STATUS_COMPLETE :
                $ret = 'complete';
                break;

            case STATUS_CANCELLED :
                $ret = 'cancelled';
                break;

            case STATUS_ERROR :
                $ret = 'error';
                break;

            default :
                $ret = 'invalid';
                break;
        }

        return $ret;

    }

    public function ready(){

        return (($this->status === STATUS_QUEUED || $this->status === STATUS_QUEUED_RETRY) && time() >= $this->start);

    }

    /**
     * Returns boolean indicating if a job has expired
     *
     * Expired jobs are completed jobs that have an expire param > 0 or are in an error state, and the expire time has passed.
     * @return boolean
     */
    public function expired(){

        return ($this->status === STATUS_ERROR
            || ($this->status === STATUS_COMPLETE && $this->expire > 0 && time() >= $this->expire));

    }
}