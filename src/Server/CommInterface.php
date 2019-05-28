<?php

namespace Hazaar\Warlock\Server;

interface CommInterface {

    public function recv(&$buf);

    public function send($command, $payload = NULL);

    public function disconnect();

}
