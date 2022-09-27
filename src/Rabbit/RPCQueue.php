<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\ReplyQueueInterface;
use AllenJB\Queues\RPCQueueInterface;
use Bunny\Channel;
use React\Promise\PromiseInterface;

class RPCQueue implements RPCQueueInterface
{

    protected ReplyQueueInterface $replyQueue;

    protected Queue $publishQueue;

    protected Channel $bunnyChannel;


    public function __construct(string $name, Channel $bunnyChannel, int $apiPort)
    {
        $this->bunnyChannel = $bunnyChannel;
        $this->publishQueue = new Queue($name, $bunnyChannel, $apiPort);
    }


    public function publish(QueueMessage $message): PromiseInterface
    {
        $replyQueue = $this->getReplyQueue();
        $message = $message->withCorrelationId($replyQueue->getCorrelationId());
        $message = $message->withReplyTo($replyQueue->getName());
        return $this->publishQueue->publish($message);
    }


    public function consume(callable $callback, float $timeoutSecs, ?float $pollIntervalSecs = null): void
    {
        $this->getReplyQueue()->consume($callback, $timeoutSecs);
    }


    public function setExpectedResponseCount(?int $count): void
    {
        $this->getReplyQueue()->setExpectedResponseCount($count);
    }


    public function incrementExpectedResponseCount(int $by = 1): void
    {
        $this->getReplyQueue()->incrementExpectedResponseCount($by);
    }

    public function setReplyQueue(ReplyQueueInterface $replyQueue): void
    {
        if (isset($this->replyQueue)) {
            trigger_error("Reply queue already initialized! Overriding it now may cause unexpected behavior", E_USER_WARNING);
        }
        $this->replyQueue = $replyQueue;
    }

    public function getReplyQueue(): ReplyQueueInterface
    {
        if (! isset($this->replyQueue)) {
            $this->replyQueue = new ReplyQueue(null, $this->bunnyChannel);
        }
        return $this->replyQueue;
    }


}
