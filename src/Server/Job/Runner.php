<?php

namespace Hazaar\Warlock\Server\Job;

class Runner extends \Hazaar\Warlock\Server\Job {

    public function init(){

        return array(
            'when' => array(
                'type' => 'Hazaar\Cron',
                'prepare' => function($value){
                    if(($start = strtotime($value)) === false)
                        return $value;
                    $this->start = $start;
                    return null;
                }
            ),
            'type' => array('value' => 'runner'),
            'timeout' => array(
                'type' => 'int',
                'default' => 60
            ),
            'exec' => 'mixed'
        );

    }

    public function touch(){

        if($this->when)
            $this->start = $this->when->getNextOccurrence();

        return $this->start;

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

    public function timeout(){

        return (time() >= ($this->process->start + $this->timeout));

    }

}