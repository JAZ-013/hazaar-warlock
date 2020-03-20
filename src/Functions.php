<?php

/**
 * Warlock Helper Functions
 */

 /**
  * Rotate a log file
  * 
  * This function will rename a log file with a sequential suffix up to a certain number.  So a file name server.log will become
  * server.log.1, if server.log.1 exists it will be renamed to server.log.2, and so on.
  *
  * @param $file string The file to rename
  * @param $file_count integer The maximum number of files.  Default: 1
  * @param $offset integer Rotation offset number to start from.  Also used as an internal counter when being called recursively.
  */
function rotateLogFile($file, $file_count = 1, $offset = 0){

    $c = $file . (($offset > 0) ? '.' . $offset : '');

    if(!\file_exists($c))
        return false;

    if($i < $logfiles)
        rotateLogFile($file, $logfiles, ++$offset);

    rename($c, $file . '.' . $i);  

    return true;

}
