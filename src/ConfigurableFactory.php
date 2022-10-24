<?php
declare(strict_types=1);

namespace AllenJB\Queues;

class ConfigurableFactory implements QueueFactoryInterface
{

    protected QueueFactoryInterface $defaultFactory;

    protected QueueFactoryInterface $normalQueueFactory;

    protected QueueFactoryInterface $delayedQueueFactory;

    protected QueueFactoryInterface $rpcQueueFactory;

    protected QueueFactoryInterface $schedulableQueueFactory;


    public function __construct(QueueFactoryInterface $defaultFactory)
    {
        $this->defaultFactory = $defaultFactory;
    }


    public function setNormalQueueFactory(QueueFactoryInterface $queueFactory): void
    {
        $this->normalQueueFactory = $queueFactory;
    }


    public function setDelayedQueueFactory(QueueFactoryInterface $queueFactory): void
    {
        $this->delayedQueueFactory = $queueFactory;
    }


    public function setRpcQueueFactory(QueueFactoryInterface $queueFactory): void
    {
        $this->rpcQueueFactory = $queueFactory;
    }


    public function setSchedulableQueueFactory(QueueFactoryInterface $queueFactory): void
    {
        $this->schedulableQueueFactory = $queueFactory;
    }


    public function create(string $queueName): QueueInterface
    {
        if (isset($this->normalQueueFactory)) {
            return $this->normalQueueFactory->create($queueName);
        }
        return $this->defaultFactory->create($queueName);
    }


    public function createDelayed(string $queueName, int $delayS): QueueInterface
    {
        if (isset($this->delayedQueueFactory)) {
            return $this->delayedQueueFactory->createDelayed($queueName, $delayS);
        }
        return $this->defaultFactory->createDelayed($queueName, $delayS);
    }


    public function rpcReply(?string $queueName): ReplyQueueInterface
    {
        if (isset($this->rpcQueueFactory)) {
            return $this->rpcQueueFactory->rpcReply($queueName);
        }
        return $this->defaultFactory->rpcReply($queueName);
    }


    public function createRpc(string $queueName): RPCQueueInterface
    {
        if (isset($this->rpcQueueFactory)) {
            return $this->rpcQueueFactory->createRpc($queueName);
        }
        return $this->defaultFactory->createRpc($queueName);
    }


    public function createSchedulable(string $queueName): SchedulableQueueInterface
    {
        if (isset($this->schedulableQueueFactory)) {
            return $this->schedulableQueueFactory->createSchedulable($queueName);
        }
        return $this->defaultFactory->createSchedulable($queueName);
    }


}
