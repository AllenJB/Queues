<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\ReplyQueueInterface;
use Ramsey\Uuid\Uuid;
use React\Promise;
use React\Promise\PromiseInterface;

class ReplyQueue implements ReplyQueueInterface
{

    protected string $name;

    protected ?string $correlationId = null;

    protected \PDO $pdo;

    protected \DateTimeZone $dbTz;

    protected ?int $expectedResponseCount = null;

    protected int $responses = 0;


    public function __construct(?string $name, \PDO $pdo, \DateTimeZone $dbTz)
    {
        $this->pdo = $pdo;
        $this->dbTz = $dbTz;

        if (($name === null) || ($name === "")) {
            $name = "_rpc_replies";
            $this->name = $name;
            /**
             * @var \Ramsey\Uuid\Type\Hexadecimal|string $correlationId ramsey/uuid 3.8 returns string
             */
            $correlationId = Uuid::uuid4()->getHex();
            if (! is_string($correlationId)) {
                $correlationId = $correlationId->toString();
            }
            $this->setCorrelationId($correlationId);
            $this->declareQueue();
        } elseif (strlen($name ?? "") > 62) {
            // MySQL max is 64 chars, and we reserve 2 for the "q_" prefix
            throw new \InvalidArgumentException("Queue name max length is 62 characters");
        } else {
            $this->name = $name;
        }
    }


    protected function declareQueue(): void
    {
        $sql = "CREATE TABLE IF NOT EXISTS `q_{$this->name}` (
          `queueid` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `dt_created` datetime NOT NULL,
          `reply_to` varchar(255) DEFAULT NULL,
          `message` text,
          `locked` tinyint(1) unsigned NOT NULL DEFAULT '0',
          `attempts` int(10) unsigned NOT NULL DEFAULT '0',
          `correlation_id` varchar(255) DEFAULT NULL,
          PRIMARY KEY `queueid` (`queueid`),
          KEY `correlation_id` (`correlation_id`, `locked`)
        );";
        $this->pdo->exec($sql);
    }


    public function publish(QueueMessage $message): PromiseInterface
    {
        $params = [
            "replyTo" => $message->getReplyTo(),
            "correlationId" => ($this->correlationId ?? $message->getCorrelationId()),
            "message" => serialize($message->getData()),
        ];
        if (($params["correlationId"] ?? "") === "") {
            return Promise\reject(new \UnexpectedValueException("Reply queue message does not have a correlation id"));
        }

        try {
            $sql = "INSERT INTO `q_{$this->name}` (`dt_created`, `reply_to`, `correlation_id`, `message`)
              VALUES (NOW(), :replyTo, :correlationId, :message);";
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
        } catch (\Exception $e) {
            if (preg_match('/Table \'[a-z0-9\.]+\' doesn\'t exist/', $e->getMessage()) !== false) {
                return Promise\resolve(false);
            }

            return Promise\reject($e);
        }
        return Promise\resolve(true);
    }


    public function setCorrelationId(string $correlationId): void
    {
        $this->correlationId = $correlationId;
    }


    public function getCorrelationId(): ?string
    {
        return $this->correlationId;
    }


    public function getName(): string
    {
        return $this->name;
    }


    public function consume(callable $callback, float $timeoutSecs, float $pollIntervalSecs = 0.1): void
    {
        $tsLimit = microtime(true) + $timeoutSecs;
        $waitingReplies = 0;
        while (true) {
            $sql = "UPDATE `q_{$this->name}`
              SET `locked` = 1, `correlation_id` = :correlationId
              WHERE `locked` = 0
              AND LAST_INSERT_ID(`queueid`)
              LIMIT 1;";
            $params = [
                "correlationId" => $this->correlationId,
            ];
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
            $affected = $stmt->rowCount();

            if ($affected > 0) {
                $waitingReplies--;
                $queueId = $this->pdo->lastInsertId();

                $sql = "SELECT * FROM `q_{$this->name}` WHERE `queueid` = :queueid";
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
                $this->ack($message);

                $this->responses++;
                if (($this->expectedResponseCount !== null) && ($this->responses >= $this->expectedResponseCount)) {
                    break;
                }
            } else {
                $waitingReplies = 0;
            }

            if (microtime(true) > $tsLimit) {
                break;
            }

            // If more queue items are pending, loop immediately
            if ($waitingReplies < 1) {
                $waitingReplies = $this->getMessageCount();
            }
            if ($waitingReplies > 0) {
                continue;
            }

            usleep((int) ($pollIntervalSecs * 1000000));
        }
    }

    public function setExpectedResponseCount(?int $count): void
    {
        $this->expectedResponseCount = $count;
    }


    public function incrementExpectedResponseCount(int $by = 1): void
    {
        $this->expectedResponseCount += $by;
    }


    protected function ack(QueueMessage $message): void
    {
        // Delete locked item
        $sql = "DELETE FROM `q_{$this->name}` WHERE `queueid` = :queueid";
        $params = [
            "queueid" => $message->getId(),
        ];
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
    }


    protected function getMessageCount(): int
    {
        $sql = "SELECT COUNT(queueid) AS `c`
            FROM `q_{$this->name}`
            WHERE `locked` = 0
            AND `correlation_id` = :correlationId";
        $params = [
            "correlationId" => $this->correlationId,
        ];
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        return (int) $stmt->fetch()->c;
    }

}
