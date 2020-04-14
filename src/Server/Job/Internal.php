<?php

namespace Hazaar\Warlock\Server\Job;

class Internal extends \Hazaar\Warlock\Server\Job {

    public function init(){

        return array(
            'when' => array(
                'type' => 'Hazaar\Cron',
                'prepare' => function($value){
                    if(is_numeric($value))
                        $start = intval($value);
                    elseif(($start = strtotime($value)) === false)
                        return $value;
                    $this->start = $start;
                    return null;
                }
            ),
            'type' => array('value' => 'internal'),
            'exec' => 'mixed'
        );

    }

    public function touch(){

        if($this->when)
            $this->start = $this->when->getNextOccurrence();

        return $this->start;

    }

}
