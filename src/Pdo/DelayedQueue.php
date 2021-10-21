<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\UnsupportedOperationException;
use React\Promise\PromiseInterface;

class DelayedQueue extends Queue
{

    /**
     * @var int Queue delivery delay in seconds
     */
    protected $delayS;


    /**
     * @param string $name
     * @param \PDO $pdo
     * @param \DateTimeZone $dbTz
     * @param int $delayS Queue delivery delay in seconds
     */
    public function __construct(string $name, \PDO $pdo, \DateTimeZone $dbTz, int $delayS)
    {
        $this->delayS = $delayS;
        parent::__construct($name, $pdo, $dbTz);
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
