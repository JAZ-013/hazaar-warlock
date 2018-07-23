<?php

namespace Hazaar\Warlock\Server;

use \Hazaar\Warlock\Server\Master;

class Process extends \Hazaar\Model\Strict {

    private $config;

    private $log;

    private $process;

    private $pipes;

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
                    return $this->status['pid'];
                }
            ),
            'exitcode' => array(
                'type' => 'int',
                'read' => function(){
                    return $this->status['exitcode'];
                }
            ),
            'status' => array(
                'type' => 'array',
                'read' => function(){
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

        $this->log->write(W_DEBUG, 'Exec: ' . $proc_cmd);

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

    public function start($output){

        $this->log->write(W_DECODE, 'OUT -> ' . $output);

        fwrite($this->pipes[0], $output);

        fclose($this->pipes[0]);

    }

    public function terminate(){

        $this->log->write(W_DEBUG, 'TERMINATE: PID=' . $this->pid);

        if(substr(PHP_OS, 0, 3) == 'WIN')
            $result = exec('taskkill /F /T /PID ' . $this->pid);
        else
            $result = proc_terminate($this->process);

        return $result;

    }

    function close(){

        //Make sure we close all the pipes
        foreach($this->pipes as $sid => $pipe) {

            if($sid === 0)
                continue;

            if($input = stream_get_contents($pipe))
                echo str_repeat('-', 30) . "\n" . $input . "\n" . str_repeat('-', 30) . "\n";

            fclose($pipe);

        }

        proc_close($this->process);

    }

}