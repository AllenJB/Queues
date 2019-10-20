<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use AllenJB\Queues\QueueFactoryInterface;
use AllenJB\Queues\QueueInterface;
use AllenJB\Queues\ReplyQueueInterface;
use AllenJB\Queues\RPCQueueInterface;
use Bunny\Channel;

class QueueFactory implements QueueFactoryInterface
{

    protected $apiPort;

    protected $bunnyChannel;


    public function __construct(Channel $bunnyChannel, int $apiPort)
    {
        $this->apiPort = $apiPort;
        $this->bunnyChannel = $bunnyChannel;
    }


    public function create(string $queueName): QueueInterface
    {
        return new Queue($queueName, $this->bunnyChannel, $this->apiPort);
    }


    public function createDelayed(string $queueName, int $delayS): QueueInterface
    {
        return new DelayedQueue($queueName, $this->bunnyChannel, $this->apiPort, $delayS);
    }


    public function rpcReply(?string $queueName): ReplyQueueInterface
    {
        return new ReplyQueue($queueName, $this->bunnyChannel);
    }


    public function createRpc(string $queueName): RPCQueueInterface
    {
        return new RPCQueue($queueName, $this->bunnyChannel, $this->apiPort);
    }


}
