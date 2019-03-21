<?php

namespace Hazaar\Warlock\Server\Job;

class Service extends \Hazaar\Warlock\Server\Job {

    public function init(){

        return array(
            'name' => 'string',
            'type' => array('value' => 'service'),
            'enabled' => array(
                'type' => 'boolean',
                'default' => true
            ),
            'dynamic' => array(
                'type' => 'boolean',
                'default' => false
            ),
            'detach' => array(
                'type' => 'boolean',
                'default' => false
            ),
            'parent' => array(
                'type' => 'Hazaar\Warlock\Server\Client'
            )
        );

    }

    public function registerClient(\Hazaar\Warlock\Server\Client $client){

        if(!parent::registerClient($client))
            return false;

        $client->name = $this->name;

        return true;

    }

}
