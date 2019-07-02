<?php

namespace Hazaar\Warlock;

/**
 * Default configuration for Warlock server.
 *
 * @author Jamie Carl <jamie@hazaarlabs.com>
 */
class Config {

    static public $default_config = array(
         'sys' => array(
             'id' => 0,
             'application_name' => NULL,
             'autostart' => FALSE,
             'pid' => 'warlock.pid',
             'timezone' => 'UTC',
             'date_format' => 'c'
         ),
         'server' => array(
             'listen' => '127.0.0.1',
             'port' => 8000,
             'encoded' => TRUE
         ),
         'cluster' => array(
            'name' => null,
            'access_key' => '0000',
            'connect_timeout' => 5,
            'frame_lifetime' => 15,
            'peers' => array()
         ),
         'kvstore' => array(
            'enabled' => FALSE,
            'persist' => FALSE,
            'namespace' => 'default',
            'compact' => 0
         ),
         'client' => array(
             'connect' => TRUE,
             'server' => NULL,
             'port' => NULL,
             'ssl' => FALSE,
             'websockets' => TRUE,
             'url' => NULL,
             'check' => 60,
             'ping' => array(
                'wait' => 5,
                'count' => 3
             ),
             'reconnect' => array(
                'enabled' => TRUE,
                'delay' => 0,
                'retries' => 0
             ),
             'admin' => array(
                 'trigger' => 'warlockadmintrigger',
                 'key' => '0000'
             )
         ),
         'timeouts' => array(
             'startup' => 1000,
             'connect'   => 5
         ),
         'log' => array(
             'level' => 'W_ERR',
             'file' => 'warlock.log',
             'error' => 'warlock-error.log',
             'rrd' => 'warlock.rrd'
         ),
         'runner' => array(
             'job' => array(
                 'retries' => 5,
                 'retry' => 30,
                 'expire' => 10
             ),
             'process' => array(
                 'timeout' => 30,
                 'limit' => 5,
                 'exitWait' => 30,
                 'php_binary' => NULL
             ),
             'service' => array(
                 'restarts' => 5,
                 'disable' => 300
             )
         ),
         'signal' => array(
            'cleanup' => TRUE,
            'queue_timeout' => 5
         )
     );

}
