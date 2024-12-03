<?php

namespace think\swoole\ipc\driver;

use RuntimeException;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use think\swoole\coroutine\Barrier;
use think\swoole\ipc\Driver;
use think\swoole\packet\Buffer;
use Throwable;

class UnixSocket extends Driver
{
    public const HEADER_SIZE   = 4;
    public const HEADER_STRUCT = 'Nlength';
    public const HEADER_PACK   = 'N';

    public function getType()
    {
        return SWOOLE_IPC_UNIXSOCK;
    }

    public function prepare(Pool $pool)
    {

    }

    public function subscribe()
    {
        Coroutine::create(function () {
            $socket = $this->getSocket($this->workerId);
            $packet = null;
            while ($data = $socket->recv()) {
                try {
                    begin:
                    if (empty($packet)) {
                        [$packet, $data] = $this->unpack($data);
                    }

                    $response = $packet->write($data);

                    if (!empty($response)) {
                        $packet  = null;
                        $message = unserialize($response);
                        $this->manager->triggerEvent('message', $message);
                    }

                    if (!empty($data)) {
                        goto begin;
                    }
                } catch (Throwable) {
                    $packet = null;
                }
            }
        });
    }

    public function publish($workerId, $message)
    {
        Barrier::run(function () use ($workerId, $message) {
            $socket = $this->getSocket($workerId);
            $data   = $this->pack(serialize($message));

            $dataSize  = strlen($data);
            $chunkSize = 1024 * 64;//每次最多发送64K数据

            $sendSize = 0;
            do {
                if (!$socket->send(substr($data, $sendSize, $chunkSize))) {
                    break;
                }
            } while (($sendSize += $chunkSize) < $dataSize);
        });
    }

    /**
     * @param $data
     * @return array<Buffer|string>
     */
    protected function unpack($data)
    {
        $header = unpack(self::HEADER_STRUCT, substr($data, 0, self::HEADER_SIZE));

        if ($header === false) {
            throw new RuntimeException('Invalid Header');
        }

        $packet = new Buffer($header['length']);
        $data   = substr($data, self::HEADER_SIZE);

        return [$packet, $data];
    }

    protected function pack($data)
    {
        return pack(self::HEADER_PACK, strlen($data)) . $data;
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
