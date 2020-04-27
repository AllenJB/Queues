<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\RPCQueueInterface;
use Bunny\Channel;
use React\Promise\PromiseInterface;

class RPCQueue implements RPCQueueInterface
{

    protected $replyQueue;

    protected $publishQueue;

    protected $correlationId;


    public function __construct(string $name, Channel $bunnyChannel, int $apiPort)
    {
        $this->replyQueue = new ReplyQueue(null, $bunnyChannel);
        $this->correlationId = $this->replyQueue->getCorrelationId();
        $this->publishQueue = new Queue($name, $bunnyChannel, $apiPort);
    }


    public function publish(QueueMessage $message): PromiseInterface
    {
        $message = $message->withCorrelationId($this->correlationId);
        $message = $message->withReplyTo($this->replyQueue->getName());
        return $this->publishQueue->publish($message);
    }


    public function consume(callable $callback, float $timeoutSecs): void
    {
        $this->replyQueue->consume($callback, $timeoutSecs);
    }


    public function setExpectedResponseCount(?int $count): void
    {
        $this->replyQueue->setExpectedResponseCount($count);
    }


    public function incrementExpectedResponseCount(int $by = 1): void
    {
        $this->replyQueue->incrementExpectedResponseCount($by);
    }


}
