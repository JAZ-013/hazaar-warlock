<?php

namespace Hazaar\Warlock\Server;

interface CommInterface {

    public function processFrame(&$buf);

    public function recv(&$buf);

    public function send($packet);

    public function disconnect();

}
