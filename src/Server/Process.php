<?php

namespace Hazaar\Warlock\Server;

use \Hazaar\Warlock\Server\Master;

class Process extends \Hazaar\Model\Strict {

    private $config;

    private $log;

    private $process;

    private $pipes;

    private $buffer;

    public function init(){

        return array(
            'id' => array(
                'type' => 'string'
            ),
            'type' =>array(
                'type' => 'string'
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
            'pid' => array(
                'type' => 'int',
                'read' => function(){
                    return ake($this->status, 'pid');
                }
            ),
            'exitcode' => array(
                'type' => 'int',
                'read' => function(){
                    return ake($this->status, 'exitcode');
                }
            ),
            'status' => array(
                'type' => 'array',
                'read' => function(){
                    if(!is_resource($this->process))
                        return false;
                    return proc_get_status($this->process);
                }
            ),
            'tag' => array(
                'type' => 'string'
            ),
            'env' => array(
                'type' => 'string'
            )
        );

    }

    function construct(\Hazaar\Application\Config $config){

        $this->config = $config;

        $this->log = new \Hazaar\Warlock\Server\Logger();

        $descriptorspec = array(
            0 => array('pipe', 'r'),
            1 => array('pipe', 'w'),
            2 => array('pipe', 'w')
        );

        $env = array_filter(array_merge($_SERVER, array(
            'APPLICATION_PATH' => $this->application['path'],
            'APPLICATION_ENV' => $this->application['env'],
            'HAZAAR_SID' => $this->config->sys->id,
            'HAZAAR_ADMIN_KEY' => $this->config->admin->key,
            'USERNAME' => (array_key_exists('USERNAME', $_SERVER) ? $_SERVER['USERNAME'] : null)
        )), 'is_string');

        $cmd = realpath(LIBRAY_PATH . '/Runner.php');

        if (!$cmd || !file_exists($cmd))
            throw new \Exception('Application command runner could not be found!');

        $php_binary = $this->config->sys['php_binary'];

        if (!file_exists($php_binary))
            throw new \Exception('The PHP CLI binary does not exist at ' . $php_binary);

        if (!is_executable($php_binary))
            throw new \Exception('The PHP CLI binary exists but is not executable!');

        $proc_cmd = basename($php_binary) . ' "' . $cmd . '"';

        $this->log->write(W_DEBUG, 'EXEC=' . $proc_cmd, $this->id);

        $this->process = proc_open($proc_cmd, $descriptorspec, $pipes, dirname($php_binary), $env);

        if(is_resource($this->process)){

            $this->pipes = $pipes;

            $this->status = proc_get_status($this->process);

            $this->log->write(W_NOTICE, 'PID: ' . $this->pid, $this->id);

        }

    }

    public function is_running(){

        return is_resource($this->process);

    }

    public function getReadPipe(){

        return $this->pipes[1];

    }

    public function start($output){

        $this->log->write(W_DECODE, 'PROCESS->INIT: ' . $output, $this->tag);

        fwrite($this->pipes[0], $output . "\n");

        stream_set_blocking($this->pipes[1], false);

        stream_set_blocking($this->pipes[2], false);

    }

    public function terminate(){

        $pids = preg_split('/\s+/', `ps -o pid --no-heading --ppid $this->pid`);

        foreach($pids as $pid){

            if(!is_numeric($pid))
                continue;

            $this->log->write(W_DEBUG, 'TERMINATE: PID=' . $pid, $this->tag);

            posix_kill($pid, 15);

        }

        $this->log->write(W_DEBUG, 'TERMINATE: PID=' . $this->pid, $this->tag);

        $result = proc_terminate($this->process, 15);

        return $result;

    }

    public function readErrorPipe(){

        $read = array($this->pipes[2]);

        $write = null;

        $except = null;

        if(!(stream_select($read, $write, $except, 0, 0) > 0))
            return false;

        foreach($read as $stream) {

            $buffer =  stream_get_contents($stream);

            if(strlen($buffer) === 0)
                return false;

            return $buffer;

        }

        return false;

    }

    public function write($packet){

        $len = strlen($packet .= "\n");

        $this->log->write(W_DEBUG, "PROCESS->PIPE: BYTES=$len ID=$this->id", $this->tag);

        $this->log->write(W_DECODE, "PROCESS->PACKET: " . trim($packet), $this->tag);

        $bytes_sent = @fwrite($this->pipes[0], $packet, $len);

        if ($bytes_sent === false) {

            $this->log->write(W_WARN, 'An error occured while sending to the client. Pipe has disappeared!?', $this->tag);

            return false;

        } elseif ($bytes_sent !== $len) {

            $this->log->write(W_ERR, $bytes_sent . ' bytes have been sent instead of the ' . $len . ' bytes expected', $this->tag);

            return false;

        }

        return true;

    }

    public function close(){

        //Make sure we close all the pipes
        foreach($this->pipes as $sid => $pipe) {

            if($sid === 0)
                continue;

            if($input = stream_get_contents($pipe)){

                $this->log->write(W_WARN, 'Excess output content on closing process', $this->tag);

                echo str_repeat('-', 30) . "\n" . $input . "\n" . str_repeat('-', 30) . "\n";

            }

            fclose($pipe);

        }

        proc_close($this->process);

    }

}