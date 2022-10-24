<?php
declare(strict_types=1);

namespace AllenJB\Queues;

interface QueueFactoryInterface
{

    public function create(string $queueName): QueueInterface;

    public function createDelayed(string $queueName, int $delayS): QueueInterface;

    public function rpcReply(?string $queueName): ReplyQueueInterface;

    public function createRpc(string $queueName): RPCQueueInterface;

    public function createSchedulable(string $queueName): SchedulableQueueInterface;

}
