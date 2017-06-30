<?php

namespace Hazaar\Warlock;

/**
 * Config short summary.
 *
 * Config description.
 *
 * @version 1.0
 * @author jamie
 */
class Config {

    static public $default_config = array(
         'sys' => array(
             'id' => 0,
             'autostart' => FALSE,
             'pid' => 'warlock.pid',
             'cleanup' => TRUE,
             'timezone' => 'UTC',
             'php_binary' => null
         ),
         'server' => array(
             'listen' => '127.0.0.1',
             'port' => 8000,
             'encoded' => TRUE,
             'win_bg' => false
         ),
         'client' => array(
             'port' => null,
             'server' => null
         ),
         'timeouts' => array(
             'startup' => 1000,
             'listen' => 60,
             'connect'   => 5,
             'subscribe' => 60
         ),
         'admin' => array(
             'trigger' => 'warlockadmintrigger',
             'key' => '0000'
         ),
         'log' => array(
             'level' => 'W_ERR',
             'file' => 'warlock.log',
             'error' => 'warlock-error.log',
             'rrd' => 'warlock.rrd'
         ),
         'job' => array(
             'retries' => 5,
             'retry' => 30,
             'expire' => 10
         ),
         'exec' => array(
             'timeout' => 30,
             'limit' => 5
         ),
         'service' => array(
             'restarts' => 5,
             'disable' => 300
         ),
         'event' => array(
             'queue_timeout' => 5
         )
     );

}