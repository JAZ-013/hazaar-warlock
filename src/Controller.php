<?php

namespace Hazaar\Warlock;

class Controller extends \Hazaar\Controller\Action {

    private $config;

    private $control;

    public function init() {

        $this->control = new \Hazaar\Warlock\Control();

        $this->layout('@controlpanel');

    }

    public function index() {

        $params = $this->request->getParams();

        $this->view->addHelper('jQuery');

        $this->view->addHelper('widget', array('theme' => 'ui-lightness'));

        $this->view->addHelper('warlock');

        $this->view->requires('controlpanel.js');

        $this->view->link('controlpanel.css');

        $this->view->tabs = array(
            'dashboard' => 'Dashboard',
            'procs' => 'Processes',
            'services' => 'Services',
            'jobs' => 'Jobs',
            'clients' => 'Clients',
            'events' => 'Events',
            'log' => 'Log'
        );

        $this->view->current = (array_key_exists('tab', $params) ? $params['tab'] : 'dashboard');

        $this->view('@' . $this->view->current);

        $this->view->status = $this->control->status();

        $this->view->service = array(
            'running' => $this->control->isRunning()
        );

        $rrd = new \Hazaar\File\RRD(\Hazaar\Application::getInstance()->runtimePath($this->control->config->log->rrd));

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

        $this->view->warlockadmintrigger = $this->control->config->admin->trigger;

        $this->view->admin_key = $this->control->config->admin->key;

        if ($this->view->current == 'log' && $file = $this->control->config->log->file){

            if(file_exists($file))
                $this->view->log = file_get_contents(\Hazaar\Application::getInstance()->runtimePath($file));

        }

    }

    public function start() {

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        if ($this->control->start())
            $out->result = 'ok';

        return $out;

    }

    public function stop() {

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        if ($this->control->stop())
            $out->result = 'ok';

        return $out;

    }

    public function subscribe() {

        $params = $this->request->getParams();

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        $out->setHeaders();

        if (! array_key_exists('ClientID', $params)) {

            $out->message = 'No client ID specified!';

        } elseif (! array_key_exists('event', $params)) {

            $out->message = 'No event name specified!';

        } elseif (! $this->control->isRunning()) {

            $out->message = 'Warlock is not running on this host.';

        } elseif (! $this->control->connected()) {

            $out->message = 'No connection to Warlock server!';

        } else {

            $filter = (array_key_exists('filter', $params) ? $params['filter'] : NULL);

            while (($event = $this->control->subscribe($params['ClientID'], $params['event'], $filter)) === NULL) {

                /*
                 * Trick to get PHP to detect aborted connections.
                 * Browsers should ignore spaces that occur before JSON responses.
                 */
                echo ' ';

                ob_flush();

                flush();
            }

            if ($event === FALSE) {

                $out->message = 'Server returned an error!';

            } elseif ($event == 'ping') {

                $out->result = 'ping';

            } else {

                $out->result = 'ok';

                $out->event = $event;

            }

        }

        return $out;

    }

    public function trigger() {

        $params = $this->request->getParams();

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        if (! array_key_exists('event', $params)) {

            $out->reason = 'No event name specified!';

        } else {

            $data = NULL;

            if (array_key_exists('data', $params) && ! ($data = json_decode($params['data'], TRUE)))
                $data = $params['data'];

            if ($this->control->trigger($params['event'], $data))
                $out->result = 'ok';

        }

        return $out;

    }

    public function statusText($status) {

        switch ($status) {
            case 0:
                $text = 'Queued';
                break;

            case 1:
                $text = 'Retrying';
                break;

            case 2:
                $text = 'Starting';
                break;

            case 3:
                $text = 'Running';
                break;

            case 4:
                $text = 'Complete';
                break;

            case 5:
                $text = 'Cancelled';
                break;

            case 6:
                $text = 'Error';
                break;

            default:
                $text = 'Unknown';
                break;

        }

        return $text;

    }

    public function status() {

        $out = new \Hazaar\Controller\Response\Json();

        $out->populate($this->control->status());

        return $out;

    }

    public function jobs() {

        $out = new \Hazaar\Controller\Response\Json();

        $jobs = array();

        foreach ($this->control->jobs() as $job) {

            $jobs[(string) $job->id] = $job;
        }

        $out->populate($jobs);

        return $out;

    }

    public function processes() {

        $out = new Response\Json();

        $out->populate($this->control->processes());

        return $out;

    }

    public function services() {

        $out = new Response\Json();

        $out->populate($this->control->services());

        return $out;

    }

    public function ping() {

        if (!$this->request->has('id'))
            throw new \Exception('No client ID specified');

        $out = new Response\Json($this->control->ping($this->request->id));

        return $out;

    }

    /**
     * Test a service
     *
     * Developing services can be difficult to debug.  While Warlock provides exceptional error handling
     * and logging output, there is nothing that can replace stepping through with breakpoints.  The
     * \Hazaar\Warlock\Control::test() method allows a service to be executed in the frontend application
     * instance and allows debuggers to work with service code.
     *
     * @throws \Exception
     * @return Response\Json
     */
    public function test() {

        $out = new Response\Json(array(
            'result' => 'err'
        ));

        if (! $this->request->has('service'))
            throw new \Exception('No service name was provided.');

        $serviceName = $this->request->service;

        $serviceClass = ucfirst($serviceName) . 'Service';

        if (! class_exists($serviceClass)) {

            $out->reason = "Service class '$serviceClass' could not be found!";

            return $out;
        }

        $service = new $serviceClass($this->application);

        $out->results['init'] = $service->init();

        $out->results['run'] = $service->run();

        $out->results['shutdown'] = $service->shutdown();

        $out->result = 'ok';

        return $out;

    }

}
