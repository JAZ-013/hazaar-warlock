<?php

namespace Hazaar\Warlock\Server\Node;

use \Hazaar\Warlock\Server\Master;

class Process extends \Hazaar\Warlock\Server\Node {

    private $process;

    private $process_status;

    private $pipes;

    public $pid;

    public $start;

    public $tag;

    function __construct($id, $type, $application, $tag = null){

        $this->name = $id;

        $this->log = Master::$instance->log;

        $descriptorspec = array(
            0 => array('pipe', 'r'),
            1 => array('pipe', 'w'),
            2 => array('pipe', 'w')
        );

        $env = array_filter(array_merge($_SERVER, array(
            'APPLICATION_PATH' => ake($application, 'path', APPLICATION_PATH),
            'APPLICATION_ENV' => ake($application, 'env', APPLICATION_ENV),
            'HAZAAR_SID' => Master::$instance->config->sys['id'],
            'HAZAAR_ADMIN_KEY' => Master::$instance->config->admin->key,
            'USERNAME' => (array_key_exists('USERNAME', $_SERVER) ? $_SERVER['USERNAME'] : null)
        )), 'is_string');

        $cmd = realpath(LIBRAY_PATH . '/Runner.php');

        if (!$cmd || !file_exists($cmd))
            throw new \Exception('Application command runner could not be found!');

        $php_binary = Master::$instance->config->runner->process['php_binary'];

        if (!file_exists($php_binary))
            throw new \Exception('The PHP CLI binary does not exist at ' . $php_binary);

        if (!is_executable($php_binary))
            throw new \Exception('The PHP CLI binary exists but is not executable!');

        $proc_cmd = basename($php_binary) . ' "' . $cmd . '"';

        $this->log->write(W_DEBUG, 'EXEC=' . $proc_cmd, $this->name);

        $this->process = proc_open($proc_cmd, $descriptorspec, $pipes, dirname($php_binary), $env);

        if(is_resource($this->process)){

            $this->pipes = $pipes;

            $this->process_status = proc_get_status($this->process);

            $this->log->write(W_NOTICE, 'PID: ' . $this->process_status['pid'], $this->name);

            $conn = new \Hazaar\Warlock\Server\Connection($this->pipes);

            $conn->setNode($this);

            parent::__construct($conn, $type);

            $this->start = time();

        }

    }

    function __destruct(){

        $this->log->write(W_DEBUG, "PROCESS->DESTROY: HOST=$this->id", $this->name);

    }

    public function is_running(){

        return is_resource($this->process);

    }

    public function getPID(){

        return ake($this->getStatus(), 'pid');

    }

    public function  getExitCode(){

        return ake($this->getStatus(), 'exitcode');

    }

    public function getStatus(){

        if(!is_resource($this->process))
            return false;

        return proc_get_status($this->process);

    }

    public function start($output){

        $this->log->write(W_DECODE, 'PROCESS->INIT: ' . $output, $this->tag);

        fwrite($this->pipes[0], $output . "\n");

        stream_set_blocking($this->pipes[1], false);

        stream_set_blocking($this->pipes[2], false);

        return true;

    }

    public function terminate(){

        $pid = $this->getPID();

        $all_pids = preg_split('/\s+/', `ps -o pid --no-heading --ppid $pid`);

        foreach($all_pids as $running_pid){

            if(!is_numeric($running_pid))
                continue;

            $this->log->write(W_DEBUG, 'TERMINATE: PID=' . $running_pid, $this->tag);

            posix_kill($running_pid, 15);

        }

        $this->log->write(W_DEBUG, 'TERMINATE: PID=' . $pid, $this->tag);

        return posix_kill($pid);

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

        $this->log->write(W_DEBUG, "PROCESS->CLOSE: PID=$this->pid ID=$this->id", $this->name);

        $this->conn->disconnect();

        proc_close($this->process);

        $this->conn = null;

        $this->process = null;

    }

    public function processCommand($command, $payload = null){

        switch($command){

            case 'CHECK':

                $this->status = $payload;

                return true;

            default:

                $this->log->write(W_INFO, 'Got unknown command: ' . $command, $this->name);

        }

        return false;

    }

}