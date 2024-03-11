<?php
declare(strict_types=1);

namespace AllenJB\Queues\Pdo;

use PHPUnit\Framework\TestCase;
use AllenJB\Queues\QueueInterface;
use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\UnsupportedOperationException;

class QueueTest extends TestCase
{
    protected \PDO $pdo;

    protected \DateTimeZone $dbTz;

    public function setUp(): void
    {
        $sqlModes = [
            'ERROR_FOR_DIVISION_BY_ZERO',
            'NO_ZERO_DATE',
            'NO_ZERO_IN_DATE',
            'STRICT_ALL_TABLES',
            'ONLY_FULL_GROUP_BY',
            'NO_ENGINE_SUBSTITUTION',
        ];
        $initCmd = "SET sql_mode = '" . implode(',', $sqlModes) . "', time_zone='+00:00'";
        $pdoAttrs = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_EMULATE_PREPARES => false,
            \PDO::ATTR_STRINGIFY_FETCHES => false,
            \PDO::MYSQL_ATTR_DIRECT_QUERY => false,
            \PDO::MYSQL_ATTR_INIT_COMMAND => $initCmd,
        ];
        $this->pdo = new \PDO(
            "mysql:host=" . $_ENV["DB_HOST"] . ";charset=utf8mb4;dbname=" . $_ENV["DB_NAME"],
            $_ENV["DB_USER"],
            $_ENV["DB_PASS"],
            $pdoAttrs
        );
        $this->dbTz = new \DateTimeZone("UTC");
    }

    public function testImmediate(): void
    {
        $queue = new Queue("unit_test_immed", $this->pdo, $this->dbTz);
        $queue->emptyQueue();

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $message = new QueueMessage("string");
        $queue->publish($message);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange);

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(-1, $sizeChangeTotal);
        $this->assertEquals(-1, $sizeChange);

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $this->assertNotNull($queueItem);
        $this->assertNotNull($queueItem->getId());
        $this->assertEquals("string", $queueItem->getData());
        $this->assertEquals(0, $queueItem->getAttempts());
        $this->assertNull($queueItem->getReplyTo());
        $this->assertNull($queueItem->getCorrelationId());

        $queue->nack($queueItem);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange);

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $this->assertNotNull($queueItem);
        $this->assertNotNull($queueItem->getId());
        $this->assertEquals("string", $queueItem->getData());
        $this->assertEquals(1, $queueItem->getAttempts());
        $this->assertNull($queueItem->getReplyTo());
        $this->assertNull($queueItem->getCorrelationId());

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $this->assertEquals(-1, $sizeChangeTotal);
        $this->assertEquals(-1, $sizeChange);

        $queue->ack($queueItem);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(0, $sizeChangeTotal);
        $this->assertEquals(0, $sizeChange);
    }

    public function testScheduled(): void
    {
        $queue = new Queue("unit_test_sched", $this->pdo, $this->dbTz);
        $queue->emptyQueue();

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $dtDelayTo = new \DateTimeImmutable("+2 seconds");
        $dtNow = new \DateTimeImmutable();
        $this->assertGreaterThan($dtNow, $dtDelayTo);

        $message = new QueueMessage("string");
        $queue->publish($message, $dtDelayTo);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(0, $sizeChange);

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );
        $this->assertNull($queueItem);

        sleep(4);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange);

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(-1, $sizeChangeTotal);
        $this->assertEquals(-1, $sizeChange);

        $this->assertNotNull($queueItem);
        $this->assertNotNull($queueItem->getId());
        $this->assertEquals("string", $queueItem->getData());
        $this->assertEquals(0, $queueItem->getAttempts());
        $this->assertNull($queueItem->getReplyTo());
        $this->assertNull($queueItem->getCorrelationId());

        $queue->ack($queueItem);
    }

    public function testEmptyQueue(): void
    {
        $queue = new Queue("unit_test_empty", $this->pdo, $this->dbTz);
        $queue->emptyQueue();

        $this->assertEquals(0, $queue->getTotalMessageCount());

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $this->assertNull($queueItem);
    }

    public function testMaxNameLength(): void
    {
        $caught = false;
        try {
            $queue = new Queue(str_repeat("a", 63), $this->pdo, $this->dbTz);
            $queue->emptyQueue();
        } catch (\InvalidArgumentException $e) {
            $caught = true;
        }
        $this->assertTrue($caught);

        $caught = false;
        try {
            $queue = new Queue(str_repeat("a", 62), $this->pdo, $this->dbTz);
            $queue->emptyQueue();
        } catch (\InvalidArgumentException $e) {
            $caught = true;
        }
        $this->assertFalse($caught);

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $message = new QueueMessage("string");
        $queue->publish($message);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange);
    }

    public function testDelayedQueue(): void
    {
        $queue = new DelayedQueue("unit_test_delay", $this->pdo, $this->dbTz, 2);
        $queue->emptyQueue();

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $message = new QueueMessage("string");
        $queue->publish($message);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(0, $sizeChange);

        // The message should not be immediately consumed
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );
        $this->assertNull($queueItem);

        // Wait for message due time to elapse
        sleep(4);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange, "orig: {$origSize}");

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(-1, $sizeChangeTotal);
        $this->assertEquals(-1, $sizeChange);

        $this->assertNotNull($queueItem);
        $this->assertNotNull($queueItem->getId());
        $this->assertEquals("string", $queueItem->getData());
        $this->assertEquals(0, $queueItem->getAttempts());
        $this->assertNull($queueItem->getReplyTo());
        $this->assertNull($queueItem->getCorrelationId());

        $queue->ack($queueItem);
    }

    public function testDelayedQueueScheduled(): void
    {
        $queue = new DelayedQueue("unit_test_sched_delay", $this->pdo, $this->dbTz, 2);
        $queue->emptyQueue();

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $dtDelayTo = new \DateTimeImmutable("+2 seconds");
        $dtNow = new \DateTimeImmutable();
        $this->assertGreaterThan($dtNow, $dtDelayTo);

        $caught = false;
        $message = new QueueMessage("string");
        try {
            $queue->publish($message, $dtDelayTo);
        } catch (UnsupportedOperationException $e) {
            $caught = true;
        }
        $this->assertTrue($caught);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(0, $sizeChangeTotal);
        $this->assertEquals(0, $sizeChange);
    }

    public function testRpcFeatures(): void
    {
        $queue = new Queue("unit_test_rpc", $this->pdo, $this->dbTz);
        $queue->emptyQueue();

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $message = (new QueueMessage("string"))
            ->withCorrelationId("test_corr_id")
            ->withReplyTo("test_reply_to");
        $queue->publish($message);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange);

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(-1, $sizeChangeTotal);
        $this->assertEquals(-1, $sizeChange);

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $this->assertNotNull($queueItem);
        $this->assertNotNull($queueItem->getId());
        $this->assertEquals("string", $queueItem->getData());
        $this->assertEquals(0, $queueItem->getAttempts());
        $this->assertEquals("test_reply_to", $queueItem->getReplyTo());
        $this->assertEquals("test_corr_id", $queueItem->getCorrelationId());

        $queue->nack($queueItem);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $this->assertEquals(1, $sizeChangeTotal);
        $this->assertEquals(1, $sizeChange);

        /**
         * @var QueueMessage|null $queueItem
         */
        $queueItem = null;
        $queue->consume(
            function (QueueInterface $queue, QueueMessage $qMsg) use (&$queueItem) {
                $queueItem = $qMsg;
            },
            0.1
        );

        $this->assertNotNull($queueItem);
        $this->assertNotNull($queueItem->getId());
        $this->assertEquals("string", $queueItem->getData());
        $this->assertEquals(1, $queueItem->getAttempts());
        $this->assertEquals("test_reply_to", $queueItem->getReplyTo());
        $this->assertEquals("test_corr_id", $queueItem->getCorrelationId());

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $origSizeTotal = $queue->getTotalMessageCount();
        $origSize = $queue->getMessageCount();

        $this->assertEquals(-1, $sizeChangeTotal);
        $this->assertEquals(-1, $sizeChange);

        $queue->ack($queueItem);

        $sizeChangeTotal = $queue->getTotalMessageCount() - $origSizeTotal;
        $sizeChange = $queue->getMessageCount() - $origSize;

        $this->assertEquals(0, $sizeChangeTotal);
        $this->assertEquals(0, $sizeChange);
    }

}
