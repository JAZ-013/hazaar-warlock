<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock\Connection;

class Pipe implements _Interface {

    protected $id;

    protected $application;

    protected $protocol;

    protected $buffer;

    public    $bytes_received = 0;

    function __construct(\Hazaar\Application $application, \Hazaar\Application\Protocol $protocol, $guid = null) {

        $this->start = time();

        $this->application = $application;

        $this->protocol = $protocol;

        $this->id = ($guid === null ? guid() : $guid);

    }

    public function connect($application_name, $host, $port, $extra_headers = null){

        return true;

    }

    public function disconnect(){

        flush();

        return false;

    }

    public function connected(){

        return true;

    }

    public function send($command, $payload = null) {

        if(!($packet = $this->protocol->encode($command, $payload)))
            return false;

        $len = strlen($packet .= "\n");

        $attempts = 0;

        $total_sent = 0;

        while($packet){

            $attempts++;

            $bytes_sent = @fwrite(STDOUT, $packet, $len);

            if($bytes_sent <= 0 || $bytes_sent === false)
                return false;

            $total_sent += $bytes_sent;

            if($total_sent === $len) //If all the bytes sent then don't waste time processing the leftover frame
                break;

            if($attempts >= 100)
                throw new \Exception('Unable to write to pipe.  Pipe appears to be stuck.');

            $packet = substr($packet, $bytes_sent);

        }

        return true;

    }

    private function processPacket(&$buffer = null){

        if ($this->buffer) {

            $buffer = $this->buffer . $buffer;

            $this->buffer = null;

            return $this->processPacket($buffer);

        }

        if (!$buffer)
            return false;

        if(($pos = strpos($buffer, "\n")) === false)
            return true;

        $packet = substr($buffer, 0, $pos);

        if (strlen($buffer) > ($pos += 1)) {

            $this->buffer = substr($buffer, $pos);

            $buffer = '';

        }

        return $packet;

    }

    public function recv(&$payload = null, $tv_sec = 3, $tv_usec = 0) {

        while($packet = $this->processPacket()){

            if($packet === true)
                break;

            return $this->protocol->decode($packet, $payload);

        }

        $read = array(STDIN);

        $write = $except = null;

        while(stream_select($read, $write, $except, $tv_sec, $tv_usec) > 0) {

            // will block to wait server response
            $buffer = fread(STDIN, 65536);

            $this->bytes_received += ($bytes_received = strlen($buffer));

            if($bytes_received > 0) {

                if(($packet = $this->processPacket($buffer)) === true)
                    continue;

                if($packet === false)
                    break;

                return $this->protocol->decode($packet, $payload);

            }elseif($bytes_received <= 0) {

                return false;

            }

        }

        return null;

    }

}
