<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\QueueFactoryInterface;
use AllenJB\Queues\QueueInterface;
use AllenJB\Queues\ReplyQueueInterface;
use AllenJB\Queues\RPCQueueInterface;

class QueueFactory implements QueueFactoryInterface
{

    protected $pdo;

    protected $dbTz;

    public function __construct(\PDO $pdo, \DateTimeZone $dbTz)
    {
        $this->pdo = $pdo;
        $this->dbTz = $dbTz;
    }


    public function create(string $queueName): QueueInterface
    {
        return new Queue($queueName, $this->pdo, $this->dbTz);
    }


    public function createDelayed(string $queueName, int $delayS): QueueInterface
    {
        return new DelayedQueue($queueName, $this->pdo, $this->dbTz, $delayS);
    }


    public function rpcReply(?string $queueName): ReplyQueueInterface
    {
        return new ReplyQueue($queueName, $this->pdo, $this->dbTz);
    }


    public function createRpc(string $queueName): RPCQueueInterface
    {
        return new RPCQueue($queueName, $this->pdo, $this->dbTz);
    }


}
