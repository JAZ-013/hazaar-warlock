# Configuration

## System

### id

Default: 0

Server ID is used to prevent clients from talking to the wrong server.  Set this to anything unique to identify the server within the
application context.

### application_name

Default: NULL

The application is also used to prevent clients from talking to the wrong server.

### autostart

Default: FALSE

If TRUE the Warlock\Control class will attempt to autostart the server if it is not running.

### pid

Default: warlock.pid

The name of the warlock process ID file relative to the application runtime directory.  For absolute paths prefix with /.

### timezone

Default: UTC

The timezone of the server.  This is mainly used for scheduled jobs.

## WebSocket Server

### listen

Default: 127.0.0.1

Server IP to listen on.  127.0.0.1 by default which only accept connections from localhost.  Use 0.0.0.0 to listen on all addresses.

### port

Default: 8000

Server port to listen on.  The client will automatically attempt to connect on this port unluess overridden in the client section.

### encoded

Default: TRUE

Enables/disabled packet encoding.  This allows some level of obscurity from prying eyes, however should not be relied upon as a sole means
for security.

## Signal Server

### cleanup

Default: TRUE

Enable/Disable message queue cleanup.

### queue_timeout

Default: 5

Message queue timeout.  Messages will hang around in the queue for this many seconds.  This allows late connections to still get
events and was the founding principle that allowed Warlock to work with long-polling HTTP connections.  Still very useful in the
WebSocket world though.

## Key/Value Storage Database

### enabled

Default: FALSE

Enable/disable the built-in key/value storage system.  Enabled by default.

### persist

Default: FALSE

If KVStore is enabled, this setting will enable restart persistent storage. Disabled by default.

### namespace

Default: default

The namespace to persist.  Currently only one namespace is supported.

### compact

Default: 0 (Disabled)

Interval (in seconds) at which the persistent storage will be compacted to reclaim space.  Disabled by default.

## Clients

### connect

Default: TRUE

Connect automatically on startup.  If FALSE, connect() must be called manually.

### server

Default: NULL

Server address override.  By default the client will automatically figure out the addresss based on the application config.  This
can set it explicitly.

### port

Default: NULL

Server port override.  By default the client will connect to the port in server->port.  Useful for reverse proxies or firewalls with
port forward, etc.  Allows only the port to be overridden but still auto generate the server part.

### ssl

Default: FALSE

Use SSL to connect.  (wss://)

### websockets

Default: TRUE

Use websockets.  Alternative is HTTP long-polling.

### url

Default: NULL

Resolved URL override.  This allows you to override the entire URL.  For the above auto URL generator to work, this needs to be NULL.

### check

Default: 60

Send a PING if no data is received from the client for this many seconds.

### ping

#### wait

Default: 5

Wait this many seconds for a PONG before sending another PING.

#### count

Default: 3

Disconnect after this many unanswered PING attempts

### reconnect

#### enabled

Default: TRUE

When using WebSockets, automatically reconnect if connection is lost.

#### delay

Default: 0

#### retries

Default: 0

### admin

#### trigger

Default: warlockadmintrigger

The name of the admin event trigger.  Only change this is you really know what you're doing.

#### key

Default: 0000

The admin key.  This is a simple passcode that allows admin clients to do a few more things, like start/stop services, subscribe to admin events, etc.

## Logging

### level

Default: W_ERR

Default log level.  Allowed: W_INFO, W_WARN, W_ERR, W_NOTICE, W_DEBUG, W_DECODE, W_DECODE2.

### date_format

Default: c
         
The value used to set the default time format used in log output.  This can be anything supported by `date_default_timezone_set()`.

### file

Default: warlock.log

The log file to write to in the application runtime directory.

### error

Default: warlock-error.log

The error log file to write to in the application runtime directory.  STDERR is redirected to this file.

### rrd

Default: warlock.rrd

The RRD data file.  Used to store RRD data for graphing realtime statistics.

## The Runner - Jobs and Services

### job'

#### retries

Default: 5

Retry jobs that failed this many times.

#### retry

Default: 30

Retry failed jobs after this many seconds.

#### expire

Default: 10

Completed jobs will be cleaned up from the job queue after this many seconds.

### process

#### timeout

Default: 30

Timeout for short run jobs initiated by the front end. Prevents runaway processes from hanging around.

#### limit

Default: 5

Maximum number of concurrent jobs to execute.  THIS INCLUDES SERVICES.  So if this is 5 and you have 6 services, one service will never run!

#### exitWait

Default: 30

How long the server will wait for processes to exit when shutting down.

#### php_binary

Default: NULL

Override path to the PHP binary file to use when executing jobs.

### service

#### restarts

Default: 5

Restart a failed service this many times before disabling it for a bit.

#### disable

Default: 300

Disable a failed service for this many seconds before trying to start it up again.

## Cluster

### name

Default: NULL

### access_key

Default: 0000

### connect_timeout

Default: 5

### frame_lifetime

Default: 15

### peers

Peers are an array of peers to connect to join the cluster and can contain one of more configurations of the following elements.

#### enabled

Default: true

Enable/disable the current peer.  Use this to temporarily disable a peer.

#### host

The hostname/IP of the peer to connect to.

#### port

The port to connect to on the peer.

