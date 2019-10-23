<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\ReplyQueueInterface;
use Bunny\Channel;
use Bunny\Client;
use Bunny\Message as BunnyMessage;
use Ramsey\Uuid\Uuid;
use React\Promise\PromiseInterface;

class ReplyQueue implements ReplyQueueInterface
{

    protected $name;

    protected $channel;

    protected $messageHeaders = [
    ];

    protected $correlationId = null;

    protected $expectedResponseCount = null;

    protected $responseCount = 0;


    public function __construct(?string $name, Channel $bunnyChannel)
    {
        $this->channel = $bunnyChannel;
        $this->name = $name;
        if (($this->name ?? "") === "") {
            $this->setCorrelationId(Uuid::uuid4()->getHex());

            $this->declareQueue();
        }
    }


    protected function declareQueue(): void
    {
        $retVal = $this->channel->queueDeclare("", false, false, true);
        if ($retVal instanceof PromiseInterface) {
            $retVal->then(
                function ($okFrame) {
                    $this->name = $okFrame->queue;
                }
            )->done();
        } else {
            $this->name = $retVal->queue;
        }
    }


    public function publish(QueueMessage $message): PromiseInterface
    {
        if (($message->getCorrelationId() ?? "") === "") {
            throw new \UnexpectedValueException("Reply queue message has no correlation id");
        }
        $messageHeaders = $this->messageHeaders;
        if (($message->getCorrelationId() ?? "") !== "") {
            $messageHeaders["correlation_id"] = $message->getCorrelationId();
        }
        if (($message->getReplyTo() ?? "") !== "") {
            $messageHeaders["reply_to"] = $message->getReplyTo();
            if (($messageHeaders["correlation_id"] ?? "") === "") {
                trigger_error("Message published with reply_to but no correlation_id", E_USER_WARNING);
            }
        }
        $data = serialize($message->getData());

        return $this->channel->publish($data, $messageHeaders, "", $this->name);
    }


    public function consume(callable $callback, float $timeoutSecs): void
    {
        $this->channel->consume(
            function (BunnyMessage $message, Channel $channel, Client $client) use ($callback) {
                if ($message->getHeader("correlation_id") !== $this->correlationId) {
                    $this->ack($message);
                    return;
                }
                $data = unserialize($message->content, ["allowed_classes" => true]);
                $queueMessage = (new QueueMessage($data))
                    ->withId($message->deliveryTag)
                    ->withAttempts(($message->redelivered ? 1 : 0))
                    ->withCorrelationId($message->getHeader("correlation_id"))
                    ->withReplyTo($message->getHeader("reply_to"))
                    ->withOriginal($message);

                $callback($this, $queueMessage);
                $this->ack($message);
                $this->responseCount++;

                if (($this->expectedResponseCount !== null) && ($this->responseCount >= $this->expectedResponseCount)) {
                    $client->stop();
                }
            },
            $this->name
        );
        $this->channel->getClient()->run($timeoutSecs);
    }


    public function setExpectedResponseCount(?int $count): void
    {
        $this->expectedResponseCount = $count;
    }


    public function incrementExpectedResponseCount(int $by = 1): void
    {
        $this->expectedResponseCount += $by;
    }


    protected function ack(BunnyMessage $message): void
    {
        $result = $this->channel->ack($message);
        if ($result instanceof PromiseInterface) {
            $result->done();
        }
    }


    public function setCorrelationId(string $correlationId): void
    {
        $this->correlationId = $correlationId;
        $this->messageHeaders["correlation_id"] = $correlationId;
    }


    public function getCorrelationId(): ?string
    {
        return $this->correlationId;
    }


    public function getName(): ?string
    {
        return $this->name;
    }


}
