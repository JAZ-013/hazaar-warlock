<?php
// Define path to application directory
defined('APPLICATION_PATH') || define('APPLICATION_PATH', (getenv('APPLICATION_PATH') ? getenv('APPLICATION_PATH') : realpath(dirname(__FILE__) . '/../.run')));

// Define application environment
defined('APPLICATION_ENV') || define('APPLICATION_ENV', (getenv('APPLICATION_ENV') ? getenv('APPLICATION_ENV') : 'development'));

// Composer autoloading
include APPLICATION_PATH . '/../vendor/autoload.php';

require_once('Constants.php');

// Create application, bootstrap, and run
$application = new \Hazaar\Application(APPLICATION_ENV);

$application->bootstrap(TRUE)->runStdin();