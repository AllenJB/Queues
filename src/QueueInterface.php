<?php
declare(strict_types=1);

namespace AllenJB\Queues;

use React\Promise\PromiseInterface;

interface QueueInterface
{

    public function publish(QueueMessage $message, \DateTimeImmutable $delayTo = null): PromiseInterface;


    /**
     * @param callable $callback Callback with parameters: QueueInterface $this, QueueMessage
     */
    public function consume(callable $callback, float $timeoutSecs, float $pollIntervalSecs): void;


    public function ack(QueueMessage $message): void;


    public function nack(QueueMessage $message): void;


    public function getMessageCount(): ?int;


    /**
     * Return the total message count, including delayed / scheduled items
     * @return int|null
     */
    public function getTotalMessageCount(): ?int;

}
