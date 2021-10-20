<?php
declare(strict_types=1);

namespace AllenJB\Queues;

use React\Promise\PromiseInterface;

interface RPCQueueInterface
{

    public function publish(QueueMessage $message): PromiseInterface;


    /**
     * @param callable $callback Callback with parameters: RPCQueueInterface $this, QueueMessage
     */
    public function consume(callable $callback, float $timeoutSecs, float $pollIntervalSecs): void;


    public function setExpectedResponseCount(?int $count): void;


    public function incrementExpectedResponseCount(int $by = 1): void;

}
