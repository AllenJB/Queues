<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\ReplyQueueInterface;
use AllenJB\Queues\RPCQueueInterface;
use React\Promise\PromiseInterface;

class RPCQueue implements RPCQueueInterface
{

    protected ReplyQueueInterface $replyQueue;

    protected Queue $publishQueue;

    protected \PDO $pdo;

    protected \DateTimeZone $dbTz;


    public function __construct(string $name, \PDO $pdo, \DateTimeZone $dbTz)
    {
        $this->pdo = $pdo;
        $this->dbTz = $dbTz;
        $this->publishQueue = new Queue($name, $pdo, $dbTz);
    }


    public function publish(QueueMessage $message): PromiseInterface
    {
        $message = $message->withCorrelationId($this->getReplyQueue()->getCorrelationId());
        $message = $message->withReplyTo($this->getReplyQueue()->getName());
        return $this->publishQueue->publish($message);
    }


    public function consume(callable $callback, float $timeoutSecs, ?float $pollIntervalSecs = 0.1): void
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
            $this->replyQueue = new ReplyQueue(null, $this->pdo, $this->dbTz);
        }
        return $this->replyQueue;
    }

}
