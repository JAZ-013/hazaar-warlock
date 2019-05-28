<?php
// Define path to application directory
defined('APPLICATION_PATH') || define('APPLICATION_PATH', (($path = getenv('APPLICATION_PATH'))
    ? $path : realpath(dirname(__FILE__) . '/../../../../application')));

// Define application environment
defined('APPLICATION_ENV') || define('APPLICATION_ENV', (getenv('APPLICATION_ENV') ? getenv('APPLICATION_ENV') : 'development'));

// Composer autoloading
include APPLICATION_PATH . '/../vendor/autoload.php';

require_once('Constants.php');

// Create application, bootstrap, and run
$application = new \Hazaar\Application(APPLICATION_ENV);

exit(\Hazaar\Warlock\Process::runner($application->bootstrap(true), ake($argv, 1)));