<?php

namespace Hazaar\Warlock;

if (!extension_loaded('sockets'))
    die('The sockets extension is not loaded.');

require_once('Constants.php');

if (!defined('APPLICATION_PATH')) define('APPLICATION_PATH', getApplicationPath());

if (!APPLICATION_PATH)
    die("Warlock can not start without an application path.  Make sure APPLICATION_PATH environment variable is set.\n");

if (!(is_dir(APPLICATION_PATH)
    && file_exists(APPLICATION_PATH . DIRECTORY_SEPARATOR . 'configs')
    && file_exists(APPLICATION_PATH . DIRECTORY_SEPARATOR . 'controllers')))
    die("Application path '" . APPLICATION_PATH . "' is not a valid application directory!\n");

chdir(APPLICATION_PATH);

define('APPLICATION_ENV', (getenv('APPLICATION_ENV') ? getenv('APPLICATION_ENV') : 'development'));

define('LIBRAY_PATH', realpath(dirname(__FILE__) . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'src'));

include APPLICATION_PATH . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR . 'autoload.php';

if(!class_exists('Hazaar\Loader'))
    throw new \Exception('A Hazaar loader could not be loaded!');

$reflector = new \ReflectionClass('Hazaar\Loader');

set_include_path(implode(PATH_SEPARATOR, array(
    realpath(dirname($reflector->getFileName())),
    realpath(LIBRAY_PATH . DIRECTORY_SEPARATOR . '..'),
    get_include_path()
)));

$reflector = null;

require_once('HelperFunctions.php');

$log_level = W_INFO;

$warlock = new Server\Master((in_array('-s', $argv) ? true : boolify(getenv('WARLOCK_OUTPUT') === 'file')));

exit($warlock->bootstrap()->run());
