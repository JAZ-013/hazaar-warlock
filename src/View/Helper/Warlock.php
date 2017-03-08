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

    public function import() {

        $this->requires('html');

    }

    /**
     * @detail      Initialise the view helper and include the buttons.css file.  Adds a requirement for the HTML view
     * helper.
     */
    public function init($view, $args = array()) {

        if(count($args) > 0)
            $this->js_varname = $args[0];

        $view->requires($this->application->url('hazaar/Warlock/client.js'));

        $defaults = array(
            'sys'        => array(
                'id'  => crc32(APPLICATION_PATH),
                'pid' => 'warlock.pid'
            ),
            'server'     => array(
                'listen'  => $_SERVER['SERVER_NAME'],
                'port'    => 8000,
                'encoded' => TRUE
            ),
            'websockets' => array(
                'enabled'       => true,
                'autoReconnect' => true
            )
        );

        $config = new \Hazaar\Application\Config('warlock', APPLICATION_ENV, $defaults);

        if(!$config->client->has('port'))
            $config->client['port'] = $config->server['port'];

        if(trim($config->server->listen) == '0.0.0.0')
            $host = $_SERVER['SERVER_NAME'] . ':' . $config->server->port . '/' . APPLICATION_NAME;
        else
            $host = $config->server->listen . ':' . $config->server->port . '/' . APPLICATION_NAME;

        $wsEnabled = strbool($config->websockets->enabled === true);

        $wsAutoReconnect = strbool($config->websockets->autoReconnect === true);

        $view->script("{$this->js_varname} = new HazaarWarlock('{$config->sys->id}', '$host', $wsEnabled, $wsAutoReconnect);");

        if($config->server->encoded === true)
            $view->script("{$this->js_varname}.enableEncoding();");

        if(($user = ake($_SERVER, 'REMOTE_USER')))
            $view->script("{$this->js_varname}.setUser('$user');");

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

}


