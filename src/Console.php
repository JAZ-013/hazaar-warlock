<?php

namespace Hazaar\Warlock;

class Console extends \Hazaar\Console\Module {

    public function init(){

        $this->addMenuGroup('dbi', 'Warlock');

        $this->addMenuItem('dbi', 'Overview', 'index');

        $this->addMenuItem('dbi', 'Services', 'services');

    }

    public function index(){

    }

    public function services(){

    }

}