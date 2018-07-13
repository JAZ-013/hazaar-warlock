<?php

namespace Hazaar\Warlock\Server;

class Logger {

    static private $log_level = W_INFO;

    private $__log_level;

    static public function set_default_log_level($level) {

        if (is_string($level))
            $level = constant($level);

        Logger::$log_level = $level;

    }

    function __construct($level = null){

        $this->__log_level = ($level === null) ? Logger::$log_level : $level;

    }

    public function write($level, $message, $job = NULL) {

        if ($level <= $this->__log_level) {

            echo date('Y-m-d H:i:s') . ' - ';

            $consts = get_defined_constants(TRUE);

            $label = 'NONE';

            foreach($consts['user'] as $name => $value) {

                if (substr($name, 0, 2) == 'W_' && $value == $level) {

                    $label = substr($name, 2);

                    break;
                }

            }

            if(is_array($message) || $message instanceof \stdClass)
                $message = 'Received ' . gettype($message) . "\n" . print_r($message, true);

            echo str_pad($label, 6, ' ', STR_PAD_LEFT) . ' - ' . ($job ? $job . ' - ' : '') . $message . "\n";

        }

    }

}