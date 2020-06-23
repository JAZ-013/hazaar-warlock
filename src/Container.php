<?php

namespace Hazaar\Warlock;

class Container extends Process {

    protected function connect(\Hazaar\Warlock\Protocol $protocol, $guid = null){

        return new Connection\Pipe($protocol);

    }

    public function exec($code, $params){

        $exitcode = 1;

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
