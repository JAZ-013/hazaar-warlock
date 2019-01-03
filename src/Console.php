<?php

namespace Hazaar\Warlock;

class Console extends \Hazaar\Console\Module {

    private $config;

    public function load(){

        $group = $this->addMenuItem('Warlock', 'magic');

        $group->addMenuItem('Processes', 'processes');

        $group->addMenuItem('Services', 'services');

        $group->addMenuItem('Jobs', 'jobs');

        $group->addMenuItem('Connections', 'connections');

        $group->addMenuItem('Events', 'events');

        $group->addMenuItem('Log File', 'log');

    }

    public function init(){

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, \Hazaar\Warlock\Config::$default_config);

        $this->view->addHelper('warlock');

        $this->view->link('css/controlpanel.css');

        $this->view->requires('js/controlpanel.js');

        $this->view->hazaar->set('admintrigger', $this->config->admin->trigger);

        $this->view->hazaar->set('admin_key', $this->config->admin->key);

    }

    public function status() {

        return [];

    }

    public function index(){

	    $this->view('overview');

        $rrd = new \Hazaar\File\RRD(\Hazaar\Application::getInstance()->runtimePath($this->config->log->rrd));

        $dataSources = $rrd->getDataSources();

        $graphs = array();

        foreach ($dataSources as $dsname) {

            $graph = $rrd->graph($dsname, 'permin_1hour');

            if ($dsname == 'memory') {

                $graph['interval'] = 1000000;

                $graph['unit'] = 'Bytes';

            } else {

                $graph['interval'] = 1;

                $graph['unit'] = ucfirst($dsname);

            }

            $graphs[$dsname] = $graph;

            $data = array();

            foreach ($graph['ticks'] as $tick => $count) {

                $data[] = array(
                    'tick' => date('H:i:s', $tick),
                    'value' => $count
                );

            }

            $graphs[$dsname]['ticks'] = $data;

        }

        $this->view->graphs = $graphs;

    }

    public function processes(){

        $this->view('procs');

    }

    public function services(){

        $this->view('services');

    }

    public function jobs(){

        $this->view('jobs');

    }

    public function connections(){

        $this->view('connections');

    }

    public function events(){

        $this->view('events');

    }

    public function log(){

        $this->view('log');

    }

}
