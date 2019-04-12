<?php

namespace Hazaar\Warlock;

class Container extends Process {

    public function exec($function, $params){

        $code = 1;

        if(is_array($function)){

            $r = new \ReflectionMethod($function[0], $function[1]);

            if(!$r->isPublic())
                throw new \Exception('Method is not public!');

            $f = file($r->getFileName());

            $start_line = $r->getStartLine() - 1;

            $end_line = $r->getEndLine();

            if(preg_match('/function\s+\w+(\(.*)/', $f[$r->getStartLine()-1], $matches))
                $f[$start_line] = 'function' . $matches[1];

            $function = implode("\n", array_splice($f, $start_line, $end_line - $start_line));

        }

        eval('$_function = ' . $function . ';');

        try{

            if(!(isset($_function) && is_callable($_function)))
                throw new \Exception('Function is not callable!');

            if(!$params)
                $params = array();

            $result = call_user_func_array($_function, $params);

            //Any of these are considered an OK response.
            if($result === NULL
            || $result === TRUE
            || $result == 0){

                $code = 0;

            } else { //Anything else is an error and we display it.

                $code = $result;

            }

        }
        catch(\Throwable $e){

            $this->log(W_ERR, $e->getMessage());

            $code = 2;

        }

        return $code;

    }

}
