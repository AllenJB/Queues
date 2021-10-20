<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use AllenJB\Queues\QueueInterface;
use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\UnsupportedOperationException;
use Bunny\Channel;
use GuzzleHttp\Client;
use React\Promise;
use React\Promise\PromiseInterface;

class Queue implements QueueInterface
{

    protected $apiPort;

    protected $name;

    protected $channel;

    protected $messageHeaders = [
        "deliver-mode" => 2,
    ];

    protected $correlationId = null;

    protected static $declaredQueues = [];


    public function __construct(string $name, Channel $bunnyChannel, int $apiPort)
    {
        $this->name = $name;
        $this->channel = $bunnyChannel;
        $this->apiPort = $apiPort;

        $this->declareQueue();
    }


    protected function declareQueue(): void
    {
        if (in_array($this->name, static::$declaredQueues, true)) {
            return;
        }
        static::$declaredQueues[] = $this->name;

        $result = $this->channel->queueDeclare($this->name, false, true, false);
        if ($result instanceof PromiseInterface) {
            $result->done();
        }
    }


    public function publish(QueueMessage $message, \DateTimeImmutable $delayTo = null): PromiseInterface
    {
        if ($delayTo !== null) {
            throw new UnsupportedOperationException("Message delays are not supported on Rabbit Queues");
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
        } elseif (($messageHeaders["correlation_id"] ?? "") !== "") {
            trigger_error("Message published with correlation_id but no reply_to", E_USER_WARNING);
        }
        $data = serialize($message->getData());

        $retval = $this->channel->publish($data, $messageHeaders, "", $this->name);
        if (! ($retval instanceof PromiseInterface)) {
            $retval = Promise\resolve(true);
        }
        return $retval;
    }


    public function consume(callable $callback, float $timeoutSecs, float $pollIntervalSecs = 0.1): void
    {
        throw new UnsupportedOperationException("Consumption is not supported by RabbitQueue");
    }


    public function ack(QueueMessage $message): void
    {
        throw new UnsupportedOperationException("Consumption is not supported by RabbitQueue");
    }


    public function nack(QueueMessage $message): void
    {
        throw new UnsupportedOperationException("Consumption is not supported by RabbitQueue");
    }


    /**
     * Returns the message count - this won't include messages awaiting ack or in uncommitted transactions
     */
    public function getMessageCount(): ?int
    {
        $bunnyProxy = new BunnyClientProxy($this->channel->getClient());

        $uri = "http://" . $bunnyProxy->getHost() . ":" . $this->apiPort . "/api/queues/"
            . urlencode($bunnyProxy->getVHost()) . "/" . urlencode($this->name);
        $options = [
            "auth" => [$bunnyProxy->getUsername(), $bunnyProxy->getPassword()],
        ];
        $httpClient = new Client();
        $response = $httpClient->get($uri, $options);
        $data = json_decode($response->getBody()->getContents(), false, 512, JSON_THROW_ON_ERROR);

        return ($data->messages_ready ?? null);
    }


    public function getTotalMessageCount(): ?int
    {
        throw new UnsupportedOperationException("Total message count is unavailable for RabbitMQ Queues");
    }

}
