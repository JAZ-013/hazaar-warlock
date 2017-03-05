<?php

namespace Hazaar\Controller;

class Warlock extends \Hazaar\Controller {

    private $action;

    private $config;

    private $control;

    public function __initialize($request) {

        $this->action = $request->getActionName();

        $this->control = new \Hazaar\Warlock\Control();

    }

    public function __run() {

        $msg = '';

        $out = NULL;

        $params = $this->request->getParams();

        switch ($this->action) {

            case 'subscribe':
                $out = $this->subscribe($params);

                break;

            case 'trigger':
                $out = $this->trigger($params);

                break;

            case 'index':
                $out = $this->controlPanel($params);

                break;

            case 'status':
                $out = $this->status($params);

                break;

            case 'jobs':
                $out = $this->jobs($params);

                break;

            case 'start':
                $out = $this->start();

                break;

            case 'stop':
                $out = $this->stop();

                break;

            case 'ping':
                if (array_key_exists('id', $params))
                    $out = $this->ping($params['id']);
                else
                    $msg = 'No client ID specified';

                break;

            case 'test':

                $out = $this->test($params['service']);

                break;

            default:
                $msg = "Action '" . $this->action . "' not implemented";

                break;
        }

        if (! $out)
            $out = new \Hazaar\Controller\Response\Text($msg);

        $out->setController($this);

        return $out;

    }

    private function start() {

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        if ($this->control->start())
            $out->result = 'ok';

        return $out;

    }

    private function stop() {

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        if ($this->control->stop())
            $out->result = 'ok';

        return $out;

    }

    private function subscribe($signal) {

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        $out->setHeaders();

        if (! array_key_exists('ClientID', $signal)) {

            $out->message = 'No client ID specified!';

        } elseif (! array_key_exists('event', $signal)) {

            $out->message = 'No event name specified!';

        } elseif (! $this->control->isRunning()) {

            $out->message = 'Warlock is not running on this host.';

        } elseif (! $this->control->connected()) {

            $out->message = 'No connection to Warlock server!';

        } else {

            $filter = (array_key_exists('filter', $signal) ? $signal['filter'] : NULL);

            while (($event = $this->control->subscribe($signal['ClientID'], $signal['event'], $filter)) === NULL) {

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

    private function trigger($signal) {

        $out = new \Hazaar\Controller\Response\Json(array(
            'result' => 'error'
        ));

        if (! array_key_exists('event', $signal)) {

            $out->reason = 'No event name specified!';

        } else {

            $data = NULL;

            if (array_key_exists('data', $signal) && ! ($data = json_decode($signal['data'], TRUE)))
                $data = $signal['data'];

            if ($this->control->trigger($signal['event'], $data))
                $out->result = 'ok';

        }

        return $out;

    }

    private function controlPanel($params) {

        $out = new Response\Layout('@Warlock/controlpanel', FALSE);

        $out->addHelper('jQuery');

        $out->addHelper('widget', array(
            'theme' => 'ui-lightness'
        ));

        $out->addHelper('warlock');

        $out->tabs = array(
            'dashboard' => 'Dashboard',
            'procs' => 'Processes',
            'services' => 'Services',
            'jobs' => 'Jobs',
            'clients' => 'Clients',
            'events' => 'Events',
            'log' => 'Log'
        );

        $out->current = (array_key_exists('tab', $params) ? $params['tab'] : 'dashboard');

        $out->add('@Warlock/' . $out->current);

        $out->status = $this->control->status();

        $out->service = array(
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

        $out->graphs = $graphs;

        $out->warlockadmintrigger = $this->control->config->admin->trigger;

        $out->admin_key = $this->control->config->admin->key;

        if ($out->current == 'log')
            $out->log = file_get_contents(\Hazaar\Application::getInstance()->runtimePath($this->control->config->log->file));

        $out->initHelpers();

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

    private function status() {

        $out = new \Hazaar\Controller\Response\Json();

        $out->populate($this->control->status());

        return $out;

    }

    private function jobs() {

        $out = new \Hazaar\Controller\Response\Json();

        $jobs = array();

        foreach ($this->control->jobs() as $job) {

            $jobs[(string) $job->id] = $job;
        }

        $out->populate($jobs);

        return $out;

    }

    private function processes() {

        $out = new Response\Json();

        $out->populate($this->control->processes());

        return $out;

    }

    private function services() {

        $out = new Response\Json();

        $out->populate($this->control->services());

        return $out;

    }

    private function ping($client) {

        $out = new Response\Json();

        if ($client) {

            $out->populate($this->control->ping($client));
        }

        return $out;

    }

    private function test($serviceName) {

        $out = new Response\Json(array(
            'result' => 'err'
        ));

        if (! $serviceName) {

            $out->reason = 'No service name was provided.';

            return $out;
        }

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
