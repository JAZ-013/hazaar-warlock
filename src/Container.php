<?php

namespace Hazaar\Warlock;

class Container extends Process {

    protected function connect($application, $protocol, $guid = null){

        return new Connection\Pipe($application, $protocol);

    }

    public function exec($function, $params){

        $exitcode = 1;

        $code = null;

        if(is_array($function)){

            $class = new \ReflectionClass($function[0]);

            $method = $class->getMethod($function[1]);

            if(!$method->isPublic())
                throw new \Exception('Method is not public!');

            $file = file($method->getFileName());

            $start_line = $method->getStartLine() - 1;

            $end_line = $method->getEndLine();

            if(preg_match('/function\s+\w+(\(.*)/', $file[$start_line], $matches))
                $file[$start_line] = 'function' . $matches[1];

            if($namespace = $class->getNamespaceName())
                $code = "namespace $namespace;\n\n";

            $code .= '$_function = ' . implode("\n", array_splice($file, $start_line, $end_line - $start_line)) . ';';

        }else $code = '$_function = ' . $function . ';';

        try{

            if($code === null)
                throw new \Exception('Unable to evaulate container code.');

            eval($code);

            if(!(isset($_function) && $_function instanceof \Closure))
                throw new \Exception('Function is not callable!');

            if(!$params)
                $params = array();

            $result = call_user_func_array($_function, $params);

            //Any of these are considered an OK response.
            if($result === NULL
            || $result === TRUE
            || $result == 0){

                $exitcode = 0;

            } else { //Anything else is an error and we display it.

                $exitcode = $result;

            }

        }
        catch(\Throwable $e){

            $this->log(W_ERR, $e->getMessage());

            $exitcode = 2;

        }

        return $exitcode;

    }

}
