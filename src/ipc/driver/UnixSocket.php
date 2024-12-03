<?php

namespace think\swoole\ipc\driver;

use Swoole\Coroutine\Socket;
use Swoole\Event;
use Swoole\Process\Pool;
use think\swoole\ipc\Driver;

class UnixSocket extends Driver
{
    public function getType()
    {
        return SWOOLE_IPC_UNIXSOCK;
    }

    public function prepare(Pool $pool)
    {

    }

    public function subscribe()
    {
        $socket = $this->getSocket($this->workerId);
        Event::add($socket, function (Socket $socket) {
            $message = unserialize($socket->recv());
            $this->manager->triggerEvent('message', $message);
        });
    }

    public function publish($workerId, $message)
    {
        $socket = $this->getSocket($workerId);
        $socket->send(serialize($message));
    }

    /**
     * @param $workerId
     * @return \Swoole\Coroutine\Socket
     */
    protected function getSocket($workerId)
    {
        return $this->manager->getPool()->getProcess($workerId)->exportSocket();
    }
}
