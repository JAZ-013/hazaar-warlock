<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock\Connection;

interface _Interface {

    function __construct(\Hazaar\Application $application, \Hazaar\Warlock\Protocol $protocol, $guid = null);

    public function connect($application_name, $host, $port, $extra_headers = null);

    public function disconnect();

    public function connected();

    public function send($command, $payload = null);

    public function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0);

}
