<?php

/**
 * @package     Socket
 */
namespace Hazaar\Warlock;

interface ServiceInterface {
    public function start();
    public function stop();
    public function state();
    public function run();
}