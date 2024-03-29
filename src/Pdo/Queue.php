<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\QueueInterface;
use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\SchedulableQueueInterface;
use React\Promise;
use React\Promise\PromiseInterface;

class Queue implements QueueInterface, SchedulableQueueInterface
{

    protected string $queueName;

    protected \PDO $pdo;

    protected \DateTimeZone $dbTz;


    public function __construct(string $queueName, \PDO $pdo, \DateTimeZone $dbTz)
    {
        // MySQL max is 64 chars, and we reserve 2 for the "q_" prefix
        if (strlen($queueName) > 62) {
            throw new \InvalidArgumentException("Queue name max length is 62 characters");
        }
        $this->queueName = $queueName;
        $this->pdo = $pdo;
        $this->dbTz = $dbTz;

        $this->declareQueue();
    }


    protected function declareQueue(): void
    {
        $sql = "CREATE TABLE IF NOT EXISTS `q_{$this->queueName}` (
          `queueid` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `dt_created` datetime NOT NULL,
          `dt_scheduled` datetime DEFAULT NULL,
          `reply_to` varchar(255) DEFAULT NULL,
          `message` text,
          `locked` tinyint(1) unsigned NOT NULL DEFAULT '0',
          `attempts` int(10) unsigned NOT NULL DEFAULT '0',
          `correlation_id` varchar(255) DEFAULT NULL,
          PRIMARY KEY `queueid` (`queueid`),
          KEY `locked` (`locked`, `dt_scheduled`)
        );";
        $this->pdo->exec($sql);
    }


    public function publish(QueueMessage $message, \DateTimeImmutable $delayTo = null): PromiseInterface
    {
        $delayToVal = null;
        if ($delayTo !== null) {
            $delayTo = $delayTo->setTimezone($this->dbTz);
            $delayToVal = $delayTo->format("Y-m-d H:i:s");
        }
        $params = [
            "dtScheduled" => $delayToVal,
            "replyTo" => $message->getReplyTo(),
            "correlationId" => $message->getCorrelationId(),
            "message" => serialize($message->getData()),
        ];

        $sql = "INSERT INTO `q_{$this->queueName}` (`dt_created`, `dt_scheduled`, `reply_to`, `correlation_id`, `message`)
          VALUES (NOW(), :dtScheduled, :replyTo, :correlationId, :message);";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        return Promise\resolve(true);
    }


    public function consume(callable $callback, float $timeoutSecs, ?float $pollIntervalSecs = 0.5): void
    {
        $tsLimit = microtime(true) + $timeoutSecs;
        while (true) {
            $consumed = $this->consumeQueueItem($callback);

            if (microtime(true) > $tsLimit) {
                break;
            }

            if ($consumed) {
                continue;
            }

            usleep((int) ($pollIntervalSecs * 1000000));
        }
    }

    protected function consumeQueueItem(callable $callback): bool
    {
        $sql = "UPDATE `q_{$this->queueName}`
              SET `locked` = 1
              WHERE `locked` = 0
              AND (`dt_scheduled` IS NULL OR `dt_scheduled` < NOW())
              AND LAST_INSERT_ID(`queueid`)
              LIMIT 1;";
        $stmt = $this->pdo->query($sql);
        if ($stmt === false) {
            $errorInfo = $this->pdo->errorInfo();
            throw new \UnexpectedValueException(
                "Queue item lock query failed: " . $errorInfo[0] . ": " . $errorInfo[2]
            );
        }
        $affected = $stmt->rowCount();

        if ($affected === 0) {
            return false;
        }

        $queueId = $this->pdo->lastInsertId();

        $sql = "SELECT * FROM `q_{$this->queueName}` WHERE `queueid` = :queueid";
        $params = [
            "queueid" => $queueId,
        ];
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        $data = unserialize($row["message"], ["allowed_classes" => true]);
        $message = (new QueueMessage($data))
            ->withId((int) $row["queueid"])
            ->withAttempts((int) $row["attempts"])
            ->withReplyTo($row["reply_to"])
            ->withCorrelationId($row["correlation_id"]);

        $callback($this, $message);
        return true;
    }

    public function drain(callable $callback): void
    {
        while (true) {
            $consumed = $this->consumeQueueItem($callback);
            if (! $consumed) {
                break;
            }
        }
    }

    public function ack(QueueMessage $message): void
    {
        // Delete locked item
        $sql = "DELETE FROM `q_{$this->queueName}` WHERE `queueid` = :queueid";
        $params = [
            "queueid" => $message->getId(),
        ];
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
    }


    public function nack(QueueMessage $message): void
    {
        // Release locked item
        $sql = "UPDATE `q_{$this->queueName}`
          SET `locked` = 0, `attempts` = `attempts` + 1
          WHERE `queueid` = :queueid";
        $params = [
            "queueid" => $message->getId(),
        ];
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
    }


    /**
     * Return message count - this only counts messages actually ready for consumption and not locked (awaiting ack)
     */
    public function getMessageCount(): ?int
    {
        $sql = "SELECT COUNT(queueid) AS `c`
            FROM `q_{$this->queueName}`
            WHERE `locked` = 0
                AND (`dt_scheduled` IS NULL OR `dt_scheduled` < NOW())";
        $stmt = $this->pdo->query($sql);
        if ($stmt === false) {
            $errorInfo = $this->pdo->errorInfo();
            throw new \UnexpectedValueException("Query failed: " . $errorInfo[0] . ": " . $errorInfo[2]);
        }
        return (int) $stmt->fetch(\PDO::FETCH_OBJ)->c;
    }


    /**
     * Returns total queue size, including messages scheduled for later delivery
     */
    public function getTotalMessageCount(): ?int
    {
        $sql = "SELECT COUNT(queueid) AS `c`
            FROM `q_{$this->queueName}`
            WHERE `locked` = 0";
        $stmt = $this->pdo->query($sql);
        if ($stmt === false) {
            $errorInfo = $this->pdo->errorInfo();
            throw new \UnexpectedValueException("Query failed: " . $errorInfo[0] . ": " . $errorInfo[2]);
        }
        return (int) $stmt->fetch(\PDO::FETCH_OBJ)->c;
    }


    public function emptyQueue(): void
    {
        $sql = "TRUNCATE TABLE `q_{$this->queueName}`";
        $this->pdo->exec($sql);
    }

}
