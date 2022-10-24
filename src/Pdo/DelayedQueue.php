<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\DelayedQueueInterface;
use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\UnsupportedOperationException;
use React\Promise\PromiseInterface;

class DelayedQueue extends Queue implements DelayedQueueInterface
{

    /**
     * @var int Queue delivery delay in seconds
     */
    protected int $delayS;


    /**
     * @param string $queueName
     * @param \PDO $pdo
     * @param \DateTimeZone $dbTz
     * @param int $delayS Queue delivery delay in seconds
     */
    public function __construct(string $queueName, \PDO $pdo, \DateTimeZone $dbTz, int $delayS)
    {
        $this->delayS = $delayS;
        parent::__construct($queueName, $pdo, $dbTz);
    }


    public function publish(QueueMessage $message, \DateTimeImmutable $delayTo = null): PromiseInterface
    {
        if ($delayTo !== null) {
            throw new UnsupportedOperationException("Message specific delays are not supported on Delayed Queues");
        }

        $delayTo = new \DateTimeImmutable("+{$this->delayS} seconds");
        return parent::publish($message, $delayTo);
    }

}
