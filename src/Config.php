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
             'id' => 0,                             //Server ID is used to prevent clients from talking to the wrong server.
             'application_name' => null,            //The application is also used to prevent clients from talking to the wrong server.
             'autostart' => FALSE,                  //If TRUE the Warlock\Control class will attempt to autostart the server if it is not running.
             'pid' => 'warlock.pid',                //The name of the warlock process ID file relative to the application runtime directory.  For absolute paths prefix with /.
             'cleanup' => TRUE,                     //Enable/Disable message queue cleanup.
             'timezone' => 'UTC',                   //The timezone of the server.  This is mainly used for scheduled jobs.
             'php_binary' => null                   //Override path to the PHP binary file to use when executing jobs.
         ),
         'server' => array(
             'listen' => '127.0.0.1',               //Server IP to listen on.  127.0.0.1 by default which only accept connections from localhost.  Use 0.0.0.0 to listen on all addresses.
             'port' => 8000,                        //Server port to listen on.  The client will automatically attempt to connect on this port unluess overridden in the client section
             'encoded' => TRUE,
             'win_bg' => false,
             'kvstore' => true
         ),
         'client' => array(
             'connect' => true,                     //Connect automatically on startup.  If false, connect() must be called manually.
             'server' => null,                      //Server address override.  By default the client will automatically figure out the addresss
                                                    //based on the application config.  This can set it explicitly.
             'port' => null,                        //Server port override.  By default the client will connect to the port in server->port.
                                                    //Useful for reverse proxies or firewalls with port forward, etc.  Allows only the port to
                                                    //be overridden but still auto generate the server part.
             'ssl' => false,                        //Use SSL to connect.  (wss://)
             'websockets' => true,                  //Use websockets.  Alternative is HTTP long-polling.
             'url' => null,                         //Resolved URL override.  This allows you to override the entire URL.  For the above auto
                                                    //URL generator to work, this needs to be NULL.
             'check' => 60,                         //Send a PING if no data is received from the client for this many seconds
             'pingWait' => 5,                       //Wait this many seconds for a PONG before sending another PING
             'pingCount' => 3,                      //Disconnect after this many unanswered PING attempts
             'reconnect' => true,                   //When using WebSockets, automatically reconnect if connection is lost.
             'reconnectDelay' => 0,
             'reconnectRetries' => 0
         ),
         'timeouts' => array(
             'startup' => 1000,                     //Timeout for Warlock\Control to wait for the server to start
             'connect'   => 5                       //Timeout for Warlock\Control attempting to connect to a server.
         ),
         'admin' => array(
             'trigger' => 'warlockadmintrigger',    //The name of the admin event trigger.  Only change this is you really know what you're doing.
             'key' => '0000'                        //The admin key.  This is a simple passcode that allows admin clients to do a few more things, like start/stop services, subscribe to admin events, etc.
         ),
         'log' => array(
             'level' => 'W_ERR',                    //Default log level.  Allowed: W_INFO, W_WARN, W_ERR, W_NOTICE, W_DEBUG, W_DECODE, W_DECODE2.
             'file' => 'warlock.log',               //The log file to write to in the application runtime directory.
             'error' => 'warlock-error.log',        //The error log file to write to in the application runtime directory.  STDERR is redirected to this file.
             'rrd' => 'warlock.rrd'                 //The RRD data file.  Used to store RRD data for graphing realtime statistics.
         ),
         'job' => array(
             'retries' => 5,                        //Retry jobs that failed this many times.
             'retry' => 30,                         //Retry failed jobs after this many seconds.
             'expire' => 10                         //Completed jobs will be cleaned up from the job queue after this many seconds.
         ),
         'exec' => array(
             'timeout' => 30,                       //Timeout for short run jobs initiated by the front end. Prevents runaway processes from hanging around.
             'limit' => 5                           //Maximum number of concurrent jobs to execute.  THIS INCLUDES SERVICES.  So if this is 5 and you have 6 services, one service will never run!
         ),
         'service' => array(
             'restarts' => 5,                       //Restart a failed service this many times before disabling it for a bit.
             'disable' => 300                       //Disable a failed service for this many seconds before trying to start it up again.
         ),
         'event' => array(
             'queue_timeout' => 5                   //Message queue timeout.  Messages will hang around in the queue for this many seconds.  This allows late connections to
                                                    //still get events and was the founding principle that allowed Warlock to work with long-polling HTTP connections.  Still
                                                    //very useful in the WebSocket world though.
         )
     );

}