<?php
/**
 * @file        Hazaar/View/Helper/Signal.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2012 Jamie Carl (http://www.hazaarlabs.com)
 */

namespace Hazaar\View\Helper;

use Hazaar\Application;

/**
 * @brief       Warlock - The background signal and command processor view helper
 *
 * @detail      This view helper is the client-side javascript required to use the built-in background signal and
 *              command processor.
 *
 * @since       2.0.0
 */
class Warlock extends \Hazaar\View\Helper {

    private $js_varname = 'warlock';

    private $config;

    public function import() {

        $this->requires('html');

    }

    /**
     * @detail      Initialise the view helper and include the buttons.css file.  Adds a requirement for the HTML view
     * helper.
     */
    public function init(\Hazaar\View\Layout $view, $args = array()) {

        if(count($args) > 0)
            $this->js_varname = $args[0];

        $view->requires($this->application->url('hazaar/warlock', 'file/client.js'));

        \Hazaar\Warlock\Config::$default_config['sys']['id'] = crc32(APPLICATION_PATH);

        $this->config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, \Hazaar\Warlock\Config::$default_config);

        $this->config->client['sid'] = $this->config->sys->id;

        if($this->config->client['port'] === null)
            $this->config->client['port'] = $this->config->server['port'];

        if($this->config->client['server'] === null){

            if(trim($this->config->server['listen']) == '0.0.0.0')
                $this->config->client['server'] = ake(explode(':', $_SERVER['HTTP_HOST']), 0, $_SERVER['SERVER_NAME']);
            else
                $this->config->client['server'] = $this->config->server['listen'];

        }

        if($this->config->client['applicationName'] === null)
            $this->config->client['applicationName'] = APPLICATION_NAME;

        $this->config->client['encoded'] = $this->config->server->encoded;

        if(($user = ake($_SERVER, 'REMOTE_USER')))
            $this->config->client['username'] = $user;

        $view->script("{$this->js_varname} = new HazaarWarlock(" . $this->config->client->toJSON() . ');');

    }

    /**
     * @detail
     *
     * @since       2.0.0
     *
     * @param       Array $args An array of optional arguments to pass to the HTML block element.
     */
    public function subscribe($event_id, $callback = NULL) {

        if(! $callback)
            $callback = 'on' . ucfirst($event_id);

        return $this->html->script("{$this->js_varname}.subscribe('$event_id', $callback);");

    }

    public function trigger($event_id, $data = NULL) {

        return $this->html->script("{$this->js_varname}.trigger('$event_id', " . json_encode($data) . ");");

    }

    public function controlPanel($code = NULL, $params = array()) {

        if(! $code) {

            return $this->html->div('The Warlock control panel requires a code to gain access.');

        }

        return $this->html->iframe(NULL, $params)
                          ->src($this->application->url('warlock', 'controlpanel', array('code' => $code)));

    }

    public function triggerURL($trigger, $data = null){

        $query = array(
            'trigger' => $trigger
        );

        if($data)
            $query['data'] = ((is_array($data) || is_object($data)) ? json_encode($data) : $data);

        $uri = 'http' . ($this->config->client['ssl'] ? 's' : '') 
            . '://' . $this->config->client['server'] 
            . ':' . $this->config->client['port']
            . '/' . $this->config->client['applicationName']
            . '/warlock?' . \http_build_query($query);

        return new \Hazaar\Http\Uri($uri);
        
    }

}


