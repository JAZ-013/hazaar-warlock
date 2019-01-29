<?php

namespace Hazaar\Warlock\Server;

class Logger {

    static private $log_level = W_INFO;

    private $__log_level;

    private $__levels = array();

    private $__str_pad = 0;

    static public function set_default_log_level($level) {

        if (is_string($level))
            $level = constant($level);

        Logger::$log_level = $level;

    }

    function __construct($level = null){

        $this->__log_level = ($level === null) ? Logger::$log_level : $level;

        $consts = get_defined_constants(TRUE);

        //Load the warlock log levels into an array.
        foreach($consts['user'] as $name => $value) {

            if (substr($name, 0, 2) == 'W_'){

                $len = strlen($this->__levels[$value] = substr($name, 2));

                if($len > $this->__str_pad)
                    $this->__str_pad = $len;

            }

        }

    }

    public function write($level, $message, $job = NULL) {

        if ($level <= $this->__log_level) {

            echo date('Y-m-d H:i:s') . ' - ';

            $label = ake($this->__levels, $level, 'NONE');

            if(is_array($message) || $message instanceof \stdClass)
                $message = 'Received ' . gettype($message) . "\n" . print_r($message, true);

            echo str_pad($label, $this->__str_pad, ' ', STR_PAD_LEFT) . ' - ' . ($job ? $job . ' - ' : '') . $message . "\n";

        }

    }

    public function getLevel(){

        return $this->__log_level;

    }

}