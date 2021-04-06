<?php
// Define path to application directory
defined('APPLICATION_PATH') || define('APPLICATION_PATH', (($path = getenv('APPLICATION_PATH'))
    ? $path : realpath(dirname(__FILE__) . '/../../../../application')));

// Define application environment
defined('APPLICATION_ENV') || define('APPLICATION_ENV', (getenv('APPLICATION_ENV') ? getenv('APPLICATION_ENV') : 'development'));

define('SERVER_PATH', realpath(dirname(__FILE__) . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'src'));

// Composer autoloading
include APPLICATION_PATH . '/../vendor/autoload.php';

$reflector = new \ReflectionClass('Hazaar\Loader');

set_include_path(implode(PATH_SEPARATOR, array(
    realpath(dirname($reflector->getFileName())),
    realpath(SERVER_PATH),
    get_include_path()
)));

$reflector = null;

require_once('Constants.php');

// Create application, bootstrap, and run
$application = new \Hazaar\Application(APPLICATION_ENV);

exit(\Hazaar\Warlock\Process::runner($application->bootstrap(true)));