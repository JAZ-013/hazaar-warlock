<?php

namespace Hazaar\Warlock\Server;

class Runner {

    public $config;

    public $log;

    /**
     * Job tags
     * @var mixed
     */
    private $tags = array();

    private $stats = array(
        'processed' => 0,       // Total number of processed jobs & events
        'execs' => 0,           // The number of successful job executions
        'lateExecs' => 0,       // The number of delayed executions
        'failed' => 0,          // The number of failed job executions
        'processes' => 0,       // The number of currently running processes
        'retries' => 0,         // The total number of job retries
        'queue' => 0,           // Current number of jobs in the queue
        'limitHits' => 0        // The number of hits on the process limiter
    );

    /**
     * JOBS & SERVICES
     */

    // Currently running processes (jobs AND services)
    private $processes = array();

    // Application services
    private $services = array();

    /**
     * QUEUES
     */

    // Main job queue
    public $jobQueue = array();

    function __construct(\Hazaar\Map $config) {

        $this->log = Master::$instance->log;

        $this->config = $config;

        if(!$this->config->process['php_binary'])
            $this->config->process['php_binary'] = dirname(PHP_BINARY)
            . DIRECTORY_SEPARATOR . 'php' . ((substr(PHP_OS, 0, 3) === 'WIN')?'.exe':'');

    }

    function __destruct() {

        if(count($this->processes) > 0) {

            $this->log->write(W_WARN, 'Killing with processes with extreme prejudice!');

            foreach($this->processes as $process)
                $process->terminate();

        }

    }

    public function start() {

        $this->log->write(W_NOTICE, 'Starting job runner');

        $options = $this->config->toDotNotation();

        foreach($options as $key => $value)
            $this->log->write(W_NOTICE, $key . ' = ' . $value);

        $services = new \Hazaar\Application\Config('service', APPLICATION_ENV);

        if (!$services->loaded())
            return false;

        $this->log->write(W_INFO, "Checking for enabled services");

        foreach($services as $name => $options) {

            $this->log->write(W_NOTICE, "Found service: $name");

            $options['name'] = $name;

            $this->services[$name] = new Service($options->toArray());

            if ($options['enabled'] === true)
                $this->serviceEnable($name);

        }

        return true;

    }

    public function stop() {

        if (count($this->jobQueue) === 0)
            return;

        $this->log->write(W_NOTICE, 'Terminating running jobs');

        foreach($this->jobQueue as $job){

            if($job->status() === 'running')
                $job->cancel();

        }

        if(($wait = $this->config->exec['exitWait']) > 0){

            $this->log->write(W_NOTICE, "Waiting for processes to exit (max $wait seconds)");

            $start = time();

            while(count($this->processes) > 0) {

                if(($start  + $wait) < time()){

                    $this->log->write(W_WARN, 'Timeout reached while waiting for process to exit.');

                    break;

                }

                $this->process();

                if (count($this->processes) === 0)
                    break;

                sleep(1);

            }

        }

    }

    private function callable($value){

        if($value instanceof \Hazaar\Map)
            $value = $value->toArray();

        if(is_array($value))
            return $value;

        if(strpos($value, '::') === false)
            return null;

        return explode('::', $value, 2);

    }

    public function processCommand(Node $node, $command, &$payload) {

        if(!$command)
            return false;

        switch ($command) {

            case 'DELAY' :

                $payload->when = time() + ake($payload, 'value', 0);

                $this->log->write(W_DEBUG, "JOB->DELAY: INTERVAL={$payload->value}");

            case 'SCHEDULE' :

                if(!property_exists($payload, 'when'))
                    throw new \Exception('Unable schedule code execution without an execution time!');

                if(!($id = $this->scheduleJob(
                    $payload->when,
                    $payload->exec,
                    $payload->application,
                    ake($payload, 'tag'),
                    ake($payload, 'overwrite', false)
                ))) throw new \Exception('Could not schedule delayed function');

                $node->send('OK', array('command' => $command, 'job_id' => $id));

                break;

            case 'CANCEL' :

                if (!$this->cancelJob($payload))
                    throw new \Exception('Error trying to cancel job');

                $this->log->write(W_NOTICE, "Job successfully cancelled");

                $node->send('OK', array('command' => $command, 'job_id' => $payload));

                break;

            case 'ENABLE' :

                $this->log->write(W_NOTICE, "ENABLE: NAME=$payload CLIENT=$node->id");

                if(!$this->serviceEnable($payload))
                    throw new \Exception('Unable to enable service ' . $payload);

                $node->send('OK', array('command' => $command, 'name' => $payload));

                break;

            case 'DISABLE' :

                $this->log->write(W_NOTICE, "DISABLE: NAME=$payload CLIENT=$node->id");

                if(!$this->serviceDisable($payload))
                    throw new \Exception('Unable to disable service ' . $payload);

                $node->send('OK', array('command' => $command, 'name' => $payload));

                break;

            case 'SERVICE' :

                $this->log->write(W_NOTICE, "SERVICE: NAME=$payload CLIENT=$node->id");

                if(!array_key_exists($payload, $this->services))
                    throw new \Exception('Service ' . $payload . ' does not exist!');

                $node->send('SERVICE', $this->services[$payload]);

                break;

            case 'SPAWN':

                if(!($name = ake($payload, 'name')))
                    throw new \Exception('Unable to spawn a service without a service name!');

                if(!($id = $this->spawn($node, $name, $payload)))
                    throw new \Exception('Unable to spawn dynamic service: ' . $name);

                $node->send('OK', array('command' => $command, 'name' => $name, 'job_id' => $id));

                break;

            case 'KILL':

                if(!($name = ake($payload, 'name')))
                    throw new \Exception('Can not kill dynamic service without a name!');

                if(!$this->kill($node, $name))
                    throw new \Exception('Unable to kill dynamic service ' . $name);

                $node->send('OK', array('command' => $command, 'name' => $payload));

                break;

            case 'SIGNAL':

                if(!($event_id = ake($payload, 'id')))
                    return false;

                //Otherwise, send this signal to any child services for the requested type
                if(!($service = ake($payload, 'service')))
                    return false;

                if(!$this->signal($node, $event_id, $service, ake($payload, 'data')))
                    throw new \Exception('Unable to signal dynamic service');

                $node->send('OK', array('command' => $command, 'name' => $payload));

                break;

            default:

                throw new \Exception('Unhandled command: ' . $command);

        }

        return true;

    }

    private function spawn(Node $client, $name, $options){

        if (!array_key_exists($name, $this->services))
            return false;

        $service = & $this->services[$name];

        $job = new Job\Service(array(
            'name' => $name,
            'start' => time(),
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'tag' => $name,
            'enabled' => true,
            'dynamic' => true,
            'detach' => ake($options, 'detach', false),
            'respawn' => false,
            'parent' => $client,
            'params' => ake($options, 'params'),
            'loglevel' => $service->loglevel
        ));

        $this->log->write(W_NOTICE, 'Spawning dynamic service: ' . $name, $job->id);

        $client->jobs[$job->id] = $this->queueAddJob($job);

        return $job->id;

    }

    private function kill(Node $client, $name){

        if (!array_key_exists($name, $this->services))
            return false;

        foreach($client->jobs as $id => $job){

            if($job->name !== $name)
                continue;

            $this->log->write(W_NOTICE, "KILL: SERVICE=$name JOB_ID=$id CLIENT={$client->id}");

            $job->cancel();

            unset($client->jobs[$id]);

        }

        return true;

    }

    private function signal(Node $client, $event_id, $service, $data = null){

        $trigger_id = uniqid();

        //If this is a message coming from the service, send it back to it's parent client connection
        if($client->type === 'SERVICE'){

            if(count($client->jobs) === 0)
                throw new \Exception('Client has no associated jobs!');

            foreach($client->jobs as $id => $job){

                if(!(array_key_exists($id, $this->jobQueue) && $job->name === $service && $job->parent instanceof Node))
                    continue;

                $this->log->write(W_NOTICE, "SERVICE->SIGNAL: SERVICE=$service JOB_ID=$id CLIENT={$client->id}");

                $job->parent->sendEvent($event_id, $trigger_id, $data);

            }

        }else{

            if(count($client->jobs) === 0)
                throw new \Exception('Client has no associated jobs!');

            foreach($client->jobs as $id => $job){

                if(!(array_key_exists($id, $this->jobQueue) && $job->name === $service))
                    continue;

                $this->log->write(W_NOTICE, "CLIENT->SIGNAL: SERVICE=$service JOB_ID=$id CLIENT={$client->id}");

                $job->sendEvent($event_id, $trigger_id, $data);

            }

        }

        return true;

    }

    public function scheduleJob($when, $exec, $application, $tag = NULL, $overwrite = false) {

        if(!property_exists($exec, 'callable')){

            $this->log->write(W_ERR, 'Unable to schedule job without function callable!');

            return false;

        }

        if($tag === null && is_array($exec->callable)){

            $tag = md5(implode('-', $exec->callable));

            $overwrite = true;

        }

        if ($tag && array_key_exists($tag, $this->tags)) {

            $job = $this->tags[$tag];

            $this->log->write(W_NOTICE, "Job already scheduled with tag $tag", $job->id);

            if ($overwrite === false){

                $this->log->write(W_NOTICE, 'Skipping', $job->id);

                return false;

            }

            $this->log->write(W_NOTICE, 'Overwriting', $job->id);

            $job->cancel();

            unset($this->tags[$tag]);

            unset($this->jobQueue[$job->id]);

        }

        $job = new Job\Runner(array(
            'when' => $when,
            'application' => array(
                'env' => $application->env
            ),
            'exec' => $exec->callable,
            'params' => ake($exec, 'params', array()),
            'timeout' => $this->config->exec->timeout
        ));

        $when = $job->touch();

        $this->log->write(W_DEBUG, "JOB: ID=$job->id");

        $this->log->write(W_DEBUG, 'WHEN: ' . date($this->config->sys['date_format'], $job->start), $job->id);

        $this->log->write(W_DEBUG, 'APPLICATION_ENV:  ' . $application->env, $job->id);

        if (!$when || $when < time()) {

            $this->log->write(W_WARN, 'Trying to schedule job to execute in the past', $job->id);

            return false;

        }

        if($tag){

            $this->log->write(W_DEBUG, 'TAG: ' . $tag, $job->id);

            $this->tags[$tag] = $job;

        }

        $this->log->write(W_NOTICE, 'Scheduling job for execution at ' . date($this->config->sys['date_format'], $when), $job->id);

        $this->queueAddJob($job);

        $this->stats['queue']++;

        return $job->id;

    }

    private function cancelJob($job_id) {

        $this->log->write(W_DEBUG, 'Trying to cancel job', $job_id);

        //If the job IS is not found return false
        if (!array_key_exists($job_id, $this->jobQueue))
            return false;

        $job =& $this->jobQueue[$job_id];

        if ($job->tag)
            unset($this->tags[$job->tag]);

        /**
         * Stop the job if it is currently running
         */
        if ($job->status === STATUS_RUNNING) {

            if ($job->process) {

                $this->log->write(W_NOTICE, 'Stopping running ' . $job->type);

                $job->process->termiante();

            } else {

                $this->log->write(W_ERR, ucfirst($job->type) . ' has running status but proccess resource was not found!');

            }

        }

        $job->status = STATUS_CANCELLED;

        // Expire the job in 30 seconds
        $job->expire = time() + $this->config->job->expire;

        return true;

    }

    /*
     * Main job processor loop This is the main loop that executed scheduled jobs.
     * It uses proc_open to execute jobs in their own process so
     * that they don't interfere with other scheduled jobs.
     */
    public function process() {

        foreach($this->jobQueue as $id => &$job){

            //Jobs that are queued and ready to execute or ready to restart an execution retry.
            if ($job->ready()){

                $now = time();

                if (count($this->processes) >= $this->config->process['limit']) {

                    $this->stats['limitHits']++;

                    $this->log->write(W_WARN, 'Process limit of ' . $this->config->process['limit'] . ' processes reached!');

                    break;

                }

                if ($job instanceof Job\Runner){

                    if ($job->retries > 0)
                        $this->stats['retries']++;

                    if($job->event === false){

                        $this->log->write(W_NOTICE, "Starting job execution", $id);

                        $this->log->write(W_DEBUG, 'NOW:  ' . date($this->config->sys['date_format'], $now), $id);

                        $this->log->write(W_DEBUG, 'WHEN: ' . date($this->config->sys['date_format'], $job->start), $id);

                        if ($job->retries > 0)
                            $this->log->write(W_DEBUG, 'RETRIES: ' . $job->retries, $id);

                    }

                    $late = $now - $job->start;

                    $job->late = $late;

                    if ($late > 0) {

                        $this->log->write(W_DEBUG, "LATE: $late seconds", $id);

                        $this->stats['lateExecs']++;

                    }

                }

                /*
                 * Open a new process to php CLI and pipe the function out to it for execution
                 */
                $process = new Node\Process($id, $job->type, $job->application, $job->tag);

                if($process->is_running()) {

                    $this->processes[$id] = $process;

                    $job->process = $process;

                    $job->status = STATUS_RUNNING;

                    $this->stats['processes']++;

                    if(!($root = getenv('APPLICATION_ROOT'))) $root = '/';

                    $payload = array(
                        'application_name' => APPLICATION_NAME,
                        'timezone' => date_default_timezone_get(),
                        'config' => array('app' => array('root' => $root))
                    );

                    $packet = null;

                    if ($job instanceof Job\Service) {

                        $payload['name'] = $job->name;

                        if($config = $job->config)
                            $payload['config'] = array_merge($payload['config'], $config);

                        $packet = Master::$protocol->encode('service', $payload);

                    } elseif ($job instanceof Job\Runner) {

                        $payload['exec'] = $job->exec;

                        if ($job->has('params') && is_array($job->params) && count($job->params) > 0)
                            $payload['params'] = $job->params;

                        $packet = Master::$protocol->encode('exec', $payload);

                    }

                    if(!Master::$cluster->addNode($process)){

                        $this->log->write(W_ERR, 'Something horrible went wrong.  Unable to add node to cluster manager!');

                        $process->terminate();

                        $job->status = STATUS_ERROR;

                        $job->process = null;

                        unset($this->processes[$id]);

                        continue;

                    }

                    $process->start($packet);

                } else {

                    $job->status = STATUS_ERROR;

                    // Expire the job when the queue expiry is reached
                    $job->expire = time() + $this->config->job->expire;

                    $this->log->start(W_ERR, 'Could not create child process.  Execution failed', $id);

                }

            } elseif ($job->expired()) { //Clean up any expired jobs (completed or errored)

                if($job->status === STATUS_CANCELLED){

                    $this->log->write(W_NOTICE, 'Killing cancelled process', $id);

                    $job->process->terminate();

                }

                $this->log->write(W_NOTICE, 'Cleaning up', $id);

                $this->stats['queue']--;

                if ($job instanceof Job\Service)
                    unset($this->services[$job->name]['job']);

                unset($this->jobQueue[$id]);

            }elseif($job->status === STATUS_RUNNING || $job->status === STATUS_CANCELLED){

                if(!is_object($job->process)){

                    $this->log->write(W_ERR, 'Service has running status, but no process linked!');

                    $job->status = STATUS_ERROR;

                    continue;

                }

                $status = $job->process->getStatus();

                if ($status['running'] === false) {

                    $this->log->write(W_DEBUG, "PROCESS->STOP: PID=$status[pid] ID=" . $job->process->id);

                    $pipe = $job->process->conn->getReadStream();

                    //Do any last second processing.  Usually shutdown log messages.
                    if($buffer = stream_get_contents($pipe))
                        $job->recv($buffer);

                    //One last check of the error buffer
                    if(($output = $job->process->readErrorPipe()) !== false)
                        $this->log->write(W_ERR, "PROCESS ERROR:\n$output");

                    //Now remove everything and clean up
                    unset($this->processes[$job->process->id]);

                    $this->stats['processes']--;

                    Master::$cluster->removeNode($job->process);

                    $job->disconnect();

                    /**
                     * Process a Service shutdown.
                     */
                    if ($job instanceof Job\Service) {

                        $name = $job->name;

                        $this->services[$name]['status'] = 'stopped';

                        $this->log->write(W_DEBUG, "SERVICE=$name EXIT=$status[exitcode]");

                        if ($status['exitcode'] !== 0 && $job->status !== STATUS_CANCELLED) {

                            $this->log->write(W_NOTICE, "Service returned status code $status[exitcode]", $name);

                            if(!($ec = ake($this->exit_codes, $status['exitcode'])))
                                $ec = array(
                                    'lvl' => W_WARN,
                                    'msg' => 'Service exited unexpectedly.',
                                    'restart' => true
                                );

                            $this->log->write($ec['lvl'], $ec['msg'], $name);

                            if(ake($ec, 'restart', false) !== true){

                                $this->log->write(W_ERR, 'Disabling the service.', $name);

                                $job->status = STATUS_ERROR;

                                continue;

                            }

                            if(ake($ec, 'reset', false) === true)
                                $job->retries = 0;

                            if ($job->retries > $this->config->service->restarts) {

                                if($job->dynamic === true){

                                    $this->log->write(W_WARN, "Dynamic service is restarting too often.  Cancelling spawn.", $name);

                                    $this->cancelJob($job->id);

                                }else{

                                    $this->log->write(W_WARN, "Service is restarting too often.  Disabling for {$this->config->service->disable} seconds.", $name);

                                    $job->start = time() + $this->config->service->disable;

                                    $job->retries = 0;

                                    $job->expire = 0;

                                }

                            } else {

                                $this->log->write(W_NOTICE, "Restarting service"
                                    . (($job->retries > 0) ? " ({$job->retries})" : null), $name);

                                if (array_key_exists($job->service, $this->services))
                                    $this->services[$job->service]['restarts']++;

                                $job->retries++;

                                $job->status = STATUS_QUEUED_RETRY;

                            }

                        } elseif ($job->respawn === true && $job->status === STATUS_RUNNING) {

                            $this->log->write(W_NOTICE, "Respawning service in "
                                . $job->respawn_delay . " seconds.", $name);

                            $job->start = time() + $job->respawn_delay;

                            $job->status = STATUS_QUEUED;

                            if (array_key_exists($job->service, $this->services))
                                $this->services[$job->service]['restarts']++;

                        } else {

                            $job->status = STATUS_COMPLETE;

                            $job->expire = time();

                        }

                    } else {

                        $this->log->write(W_NOTICE, "Process exited with return code: " . $status['exitcode'], $id);

                        if ($status['exitcode'] > 0) {

                            $this->log->write(W_WARN, 'Execution completed with error.', $id);

                            if($job->event === true) {

                                $job->status = STATUS_ERROR;

                            }elseif ($job->retries >= $this->config->job->retries) {

                                $this->log->write(W_ERR, 'Cancelling job due to too many retries.', $id);

                                $job->status = STATUS_ERROR;

                                $this->stats['failed']++;

                            }else{

                                $this->log->write(W_NOTICE, 'Re-queuing job for execution.', $id);

                                $job->status = STATUS_QUEUED_RETRY;

                                $job->start = time() + $this->config->job->retry;

                                $job->retries++;

                            }

                        } else {

                            $this->log->write(W_NOTICE, 'Execution completed successfully.', $id);

                            $this->stats['execs']++;

                            if(($next = $job->touch()) > time()){

                                $job->status = STATUS_QUEUED;

                                $job->retries = 0;

                                $this->log->write(W_NOTICE, 'Next execution at: ' . date($this->config->sys['date_format'], $next), $id);

                            }else{

                                $job->status = STATUS_COMPLETE;

                                // Expire the job in 30 seconds
                                $job->expire = time() + $this->config->job->expire;

                            }

                        }

                    }

                } elseif ($status['running'] === TRUE) {

                    try{

                        //Receive any error from STDERR
                        if(($output = $job->process->readErrorPipe()) !== false)
                            $this->log->write(W_ERR, "PROCESS ERROR:\n$output");

                    }
                    catch(\Throwable $e){

                        $this->log->write(W_ERR, 'EXCEPTION #'
                            . $e->getCode()
                            . ' on line ' . $e->getLine()
                            . ' in file ' . $e->getFile()
                            . ': ' . $e->getMessage());

                    }

                }

            } elseif ($job instanceof Job\Runner
                        && $job->status === STATUS_RUNNING
                        && $job->timeout()) {

                $this->log->write(W_WARN, "Process taking too long to execute - Attempting to kill it.", $id);

                if ($job->process->terminate())
                    $this->log->write(W_DEBUG, 'Terminate signal sent.', $id);
                else
                    $this->log->write(W_ERR, 'Failed to send terminate signal.', $id);

            }

        }

    }

    public function queueAddJob(Job $job){

        if(array_key_exists($job->id, $this->jobQueue)){

            $this->log->write(W_WARN, 'Job already exists in queue!', $job->id);

            return false;

        }

        $job->status = STATUS_QUEUED;

        $this->jobQueue[$job->id] = $job;

        $this->stats['queue']++;

        $start = date(Master::$instance->config->log['date_format'], $job->start);

        if(!$start || (is_int($start) && $start < time()))
            $start = 'Now';

        $this->log->write(W_DEBUG, "JOB->QUEUE: START=" . $start . ($job->tag ? " TAG=$job->tag" : null), $job->id);

        $this->process();

        return $job;

    }

    private function serviceEnable($name) {

        if (!array_key_exists($name, $this->services))
            return false;

        $service = $this->services[$name];

        if($service->job !== null)
            return false;

        $this->log->write(W_INFO, 'Enabling service: ' . $name . (($service->delay > 0) ? ' (delay=' . $service->delay . ')': null));

        $service->enabled = true;

        $job = new Job\Service(array(
            'name' => $service->name,
            'application' => array(
                'path' => APPLICATION_PATH,
                'env' => APPLICATION_ENV
            ),
            'tag' => $name,
            'respawn' => false,
            'loglevel' => $service->loglevel
        ));

        if($service->delay > 0)
            $job->start = time() + $service->delay;

        $this->services[$name]->job = $this->queueAddJob($job);

        return true;

    }

    private function serviceDisable($name) {

        if (!array_key_exists($name, $this->services))
            return false;

        $service = &$this->services[$name];

        if(!$service->enabled)
            return false;

        $this->log->write(W_INFO, 'Disabling service: ' . $name);

        return $service->disable($this->config->job->expire);

    }

}
