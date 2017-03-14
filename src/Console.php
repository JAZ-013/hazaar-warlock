<?php

namespace Hazaar\Warlock;

class Console extends \Hazaar\Console\Module {

    public function init(){

        $this->addMenuGroup('warlock', 'Warlock');

        $this->addMenuItem('warlock', 'Overview', 'index');

        $this->addMenuItem('warlock', 'Services', 'services');

        $this->addMenuItem('warlock', 'Connections', 'connections');

        $this->addMenuItem('warlock', 'Processes', 'processes');

        $this->addMenuItem('warlock', 'Log File', 'log');

    }

    public function index(){

    }

    public function services(){

    }

    public function connections(){

    }

    public function processes(){

    }

    public function log(){

    }

}