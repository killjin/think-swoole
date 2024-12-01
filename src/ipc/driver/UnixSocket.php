<?php

namespace think\swoole\ipc\driver;

use Swoole\Constant;
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
        $pool->set(['enable_message_bus' => true]);
        $pool->on(Constant::EVENT_MESSAGE, function (Pool $pool, string $data) {
            $message = unserialize($data);
            $this->manager->triggerEvent('message', $message);
        });
    }

    public function subscribe()
    {

    }

    public function publish($workerId, $message)
    {
        $this->manager->getPool()->sendMessage(serialize($message), $workerId);
    }

}
