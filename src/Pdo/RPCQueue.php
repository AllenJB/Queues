<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\RPCQueueInterface;
use React\Promise\PromiseInterface;

class RPCQueue implements RPCQueueInterface
{

    protected $correlationId;

    protected $replyQueue;

    protected $publishQueue;


    public function __construct(string $name, \PDO $pdo, \DateTimeZone $dbTz)
    {
        $this->replyQueue = new ReplyQueue(null, $pdo, $dbTz);
        $this->correlationId = $this->replyQueue->getCorrelationId();
        $this->publishQueue = new Queue($name, $pdo, $dbTz);
    }


    public function publish(QueueMessage $message): PromiseInterface
    {
        $message = $message->withCorrelationId($this->correlationId);
        $message = $message->withReplyTo($this->replyQueue->getName());
        return $this->publishQueue->publish($message);
    }


    public function consume(callable $callback, float $timeoutSecs, ?float $pollIntervalSecs = 0.1): void
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
