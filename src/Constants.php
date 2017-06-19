<?php

/**
 * LOGGING CONSTANTS
 */
define('W_INFO', 0);

define('W_ERR', 1);

define('W_WARN', 2);

define('W_NOTICE', 3);

define('W_DEBUG', 4);

define('W_DECODE', 5);

define('W_DECODE2', 6);

/*
 * Service Status Codes
 */
define('HAZAAR_SERVICE_ERROR', -1);

define('HAZAAR_SERVICE_INIT', 0);

define('HAZAAR_SERVICE_READY', 1);

define('HAZAAR_SERVICE_RUNNING', 2);

define('HAZAAR_SERVICE_SLEEP', 3);

define('HAZAAR_SERVICE_STOPPING', 4);

define('HAZAAR_SERVICE_STOPPED', 5);

define('HAZAAR_SCHEDULE_DELAY', 0);

define('HAZAAR_SCHEDULE_INTERVAL', 1);

define('HAZAAR_SCHEDULE_NORM', 2);

define('HAZAAR_SCHEDULE_CRON', 3);

/**
 * STATUS CONSTANTS
 */
define('STATUS_QUEUED', 0);

define('STATUS_QUEUED_RETRY', 1);

define('STATUS_STARTING', 2);

define('STATUS_RUNNING', 3);

define('STATUS_COMPLETE', 4);

define('STATUS_CANCELLED', 5);

define('STATUS_ERROR', 6);