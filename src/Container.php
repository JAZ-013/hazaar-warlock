<?php

namespace Hazaar\Warlock;

class Container extends Process {

    public function exec($function, $params){

        $code = 1;

        eval('$_function = ' . $function . ';');

        if(isset($_function) && $_function instanceof \Closure) {

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

        } else {

            $code = 2;

        }

        return $code;

    }

}
