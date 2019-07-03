<?php

namespace Hazaar\Warlock\Server;

/**
 * Signal short summary.
 *
 * Signal description.
 *
 * @version 1.0
 * @author JamieCarl
 */
class Signal {

    private $config;

    private $log;

    // The wait queue. Clients subscribe to events and are added to this array.
    private $waitQueue = array();

    // The Event queue. Holds active events waiting to be seen.
    private $eventQueue = array();

    // The global event queue.  Holds details about jobs that need to start up to process global events.
    private $globalQueue = array();

    public $stats = array(
        'events' => 0,          // The number of events triggered
        'subscriptions' => 0    // The number of waiting client connections
    );

    function __construct($config){

        $this->config = $config;

        $this->log = Master::$instance->log;

    }

    public function disconnect(Node $node){

        foreach($this->waitQueue as $event_id => &$queue){

            if(!array_key_exists($node->id, $queue))
                continue;

            $this->log->write(W_DEBUG, "CLIENT->UNSUBSCRIPE: EVENT=$event_id CLIENT=$node->id", $node->name);

            unset($queue[$node->id]);

        }

    }

    public function subscribeCallable($event_name, $callable){

        if(!is_callable($callable))
            return false;

        $this->log->write(W_DEBUG, 'SUBSCRIBE: ' . $event_name);

        $this->globalQueue[$event_name] = $callable;

        return true;

    }

    /**
     * Subscribe a client to an event
     *
     * @param mixed $client The client to subscribe
     * @param mixed $event_id The event ID to subscribe to
     * @param mixed $filter Any event filters
     */
    public function subscribe(Node $client, $event_id, $filter) {

        $this->waitQueue[$event_id][$client->id] = array(
            'client' => $client,
            'since' => time(),
            'filter' => $filter
        );

        $this->log->write(W_DEBUG, "CLIENT<-QUEUE: EVENT=$event_id COUNT=" . count($this->waitQueue[$event_id]) . " CLIENT=$client->id", $client->name);

        if ($event_id === $this->config->admin->trigger)
            $this->log->write(W_DEBUG, "ADMIN->SUBSCRIBE: CLIENT=$client->id", $client->name);
        else
            $this->stats['subscriptions']++;

        /*
         * Check to see if this subscribe request has any active and unseen events waiting for it.
         */
        $this->processEventQueue($client, $event_id, $filter);

        return true;

    }

    /**
     * Unsubscibe a client from an event
     *
     * @param mixed $client The client to unsubscribe
     * @param mixed $event_id The event ID to unsubscribe from
     *
     * @return boolean
     */
    public function unsubscribe(Node $client, $event_id) {

        if (!(array_key_exists($event_id, $this->waitQueue)
            && is_array($this->waitQueue[$event_id])
            && array_key_exists($client->id, $this->waitQueue[$event_id])))
            return false;

        $this->log->write(W_DEBUG, "CLIENT<-DEQUEUE: NAME=$event_id CLIENT=$client->id", $client->name);

        unset($this->waitQueue[$event_id][$client->id]);

        if ($event_id === $this->config->admin->trigger)
            $this->log->write(W_DEBUG, "ADMIN->UNSUBSCRIBE: CLIENT=$client->id", $client->name);
        else
            $this->stats['subscriptions']--;

        return true;

    }

    public function trigger(Node $node, $event_id, $data, $echo = false) {

        $trigger_id = uniqid();

        $this->log->write(W_NOTICE, "TRIGGER: EVENT=$event_id TRIGGER_ID=$trigger_id");

        $this->stats['events']++;

        $seen = array();

        if($echo !== true && $node->id > 0)
            $seen[] = $node->id;

        $this->eventQueue[$event_id][$trigger_id] = $payload = array(
            'id' => $event_id,
            'trigger' => $trigger_id,
            'when' => time(),
            'data' => $data,
            'seen' => $seen
        );

        if(array_key_exists($event_id, $this->globalQueue)){

            $this->log->write(W_NOTICE, 'Global event triggered', $event_id);

            $job = new Job\Runner(array(
                'application' => array(
                    'path' => APPLICATION_PATH,
                    'env' => APPLICATION_ENV
                ),
                'exec' => $this->globalQueue[$event_id],
                'params' => array($data, $payload),
                'timeout' => $this->config->exec->timeout,
                'event' => true
            ));

            $this->log->write(W_DEBUG, "JOB: ID=$job->id");

            $this->log->write(W_DEBUG, 'APPLICATION_PATH: ' . APPLICATION_PATH, $job->id);

            $this->log->write(W_DEBUG, 'APPLICATION_ENV:  ' . APPLICATION_ENV, $job->id);

            Master::$instance->queueAddJob($job);

        }

        // Check to see if there are any clients waiting for this event and send notifications to them all.
        $this->processSubscriptionQueue($event_id, $trigger_id);

        return true;

    }

    /**
     * Process the event queue for a specified client.
     *
     * This method is executed when a client connects to see if there are any events waiting in the event
     * queue that the client has not yet seen.  If there are, the first event found is sent to the client, marked
     * as seen and then processing stops.
     *
     * @param CommInterface $client
     *
     * @param string $event_id
     *
     * @param Array $filter
     *
     * @return boolean
     */
    private function processEventQueue(Node $client, $event_id, $filter = NULL) {

        if (!(array_key_exists($event_id, $this->eventQueue)
            && is_array($this->eventQueue[$event_id])
            && ($count = count($this->eventQueue[$event_id])) > 0))
            return false;

        $this->log->write(W_DEBUG, "QUEUE: EVENT=$event_id COUNT=$count");

        foreach($this->eventQueue[$event_id] as $trigger_id => &$event) {

            if (!array_key_exists('seen', $event) || !is_array($event['seen']))
                $event['seen'] = array();

            if (!in_array($client->id, $event['seen'])) {

                if ($this->filterEvent($event, $filter))
                    continue;

                if (!$client->sendEvent($event['id'], $trigger_id, $event['data']))
                    return false;

                $event['seen'][] = $client->id;

                if ($event_id != $this->config->admin->trigger)
                    $this->log->write(W_DEBUG, "SEEN: NAME=$event_id TRIGGER=$trigger_id CLIENT=" . $client->id);

            }

        }

        return true;

    }

    /**
     * Process all subscriptions for a specified event.
     *
     * This method is executed when a event is triggered.  It is responsible for sending events to clients
     * that are waiting for the event and marking them as seen by the client.
     *
     * @param string $event_id
     *
     * @param string $trigger_id
     *
     * @return boolean
     */
    private function processSubscriptionQueue($event_id, $trigger_id = NULL) {

        if (!(array_key_exists($event_id, $this->eventQueue)
            && is_array($this->eventQueue[$event_id])
            && ($count = count($this->eventQueue[$event_id])) > 0))
            return false;

        $this->log->write(W_DEBUG, "QUEUE: NAME=$event_id COUNT=$count");

        // Get a list of triggers to process
        $triggers = (empty($trigger_id) ? array_keys($this->eventQueue[$event_id]) : array($trigger_id));

        foreach($triggers as $trigger) {

            if (!isset($this->eventQueue[$event_id][$trigger]))
                continue;

            $event = &$this->eventQueue[$event_id][$trigger];

            if (!array_key_exists($event_id, $this->waitQueue))
                continue;

            foreach($this->waitQueue[$event_id] as $client_id => $item) {

                if (in_array($client_id, $event['seen'])
                    || $this->filterEvent($event, $item['filter']))
                    continue;

                if (!$item['client']->sendEvent($event_id, $trigger, $event['data']))
                    continue;

                $event['seen'][] = $client_id;

                if ($event_id != $this->config->admin->trigger)
                    $this->log->write(W_DEBUG, "SEEN: NAME=$event_id TRIGGER=$trigger CLIENT={$client_id}");

            }

        }

        return true;

    }

    /**
     * Tests whether a event should be filtered.
     *
     * Returns true if the event should be filtered (skipped), and false if the event should be processed.
     *
     * @param string $event
     *            The event to check.
     *
     * @param Array $filter
     *            The filter rule to test against.
     *
     * @return bool Returns true if the event should be filtered (skipped), and false if the event should be processed.
     */
    private function filterEvent($event, $filter = NULL) {

        if (!$filter instanceof \stdClass)
            return false;

        $this->log->write(W_DEBUG, 'Checking event filter for \'' . $event['id'] . '\'');

        foreach($filter as $field => $data) {

            $field = explode('.', $field);

            if (!$this->fieldExists($field, $event['data']))
                return true;

            $field_value = $this->getFieldValue($field, $event['data']);

            if ($data instanceof \stdClass) { // If $data is an array it's a complex filter

                foreach($data as $filter_type => $filter_value) {

                    switch ($filter_type) {
                        case 'is' :

                            if ($field_value != $filter_value)
                                return true;

                            break;

                        case 'not' :

                            if ($field_value === $filter_value)
                                return true;

                            break;

                        case 'like' :

                            if (!preg_match($filter_value, $field_value))
                                return true;

                            break;

                        case 'in' :

                            if (!in_array($field_value, $filter_value))
                                return true;

                            break;

                        case 'nin' :

                            if (in_array($field_value, $filter_value))
                                return true;

                            break;

                    }

                }

            } else { // Otherwise it's a simple filter with an acceptable value in it

                if ($field_value != $data)
                    return true;

            }

        }

        return false;

    }

    public function queueCleanup() {

        if (!is_array($this->eventQueue))
            $this->eventQueue = array();

        if($this->config->sys['cleanup'] === false)
            return;

        if (count($this->eventQueue) > 0) {

            foreach($this->eventQueue as $event_id => $events) {

                foreach($events as $id => $data) {

                    if (($data['when'] + $this->config['queue_timeout']) <= time()) {

                        if ($event_id != $this->config->admin->trigger)
                            $this->log->write(W_DEBUG, "EXPIRE: NAME=$event_id TRIGGER=$id");

                        unset($this->eventQueue[$event_id][$id]);

                    }

                }

                if (count($this->eventQueue[$event_id]) === 0)
                    unset($this->eventQueue[$event_id]);

            }

        }

    }

    private function fieldExists($search, $array) {

        reset($search);

        while($field = current($search)) {

            if (!property_exists($array, $field))
                return false;

            $array = &$array->$field;

            next($search);

        }

        return true;

    }

    private function getFieldValue($search, $array) {

        reset($search);

        while($field = current($search)) {

            if (!property_exists($array, $field))
                return false;

            $array = &$array->$field;

            next($search);

        }

        return $array;

    }

}