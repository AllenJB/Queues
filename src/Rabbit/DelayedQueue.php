<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use AllenJB\Queues\QueueMessage;
use AllenJB\Queues\UnsupportedOperationException;
use Bunny\Async\Client as AsyncClient;
use Bunny\Channel;
use React\Promise\PromiseInterface;

class DelayedQueue extends Queue
{

    protected string $destName;

    protected int $delayMS;


    public function __construct(string $name, Channel $bunnyChannel, int $apiPort, int $delayS)
    {
        $this->destName = $name;
        $this->delayMS = $delayS * 1000;

        parent::__construct($name . "_wait", $bunnyChannel, $apiPort);
    }


    protected function declareQueue(): void
    {
        $waitQueueArgs = [
            "x-dead-letter-exchange" => "ex_" . $this->destName,
            "x-message-ttl" => $this->delayMS /* ms */,
        ];

        if ($this->channel->getClient() instanceof AsyncClient) {
            $promise = $this->channel->exchangeDeclare("ex_" . $this->destName, "fanout", false, true);
            if (! ($promise instanceof PromiseInterface)) {
                throw new \UnexpectedValueException("Exchange declare did not return a promise");
            }
            $promise->then(function () {
                return $this->channel->queueDeclare($this->destName, false, true, false);
            })->then(function () {
                return $this->channel->queueBind($this->destName, "ex_" . $this->destName);
            })->then(function () use ($waitQueueArgs) {
                return $this->channel->queueDeclare($this->name, false, true, false, false, false, $waitQueueArgs);
            })->done();
        } else {
            $this->channel->exchangeDeclare("ex_" . $this->destName, "fanout", false, true);
            $this->channel->queueDeclare($this->destName, false, true, false);
            $this->channel->queueBind($this->destName, "ex_" . $this->destName);

            $this->channel->queueDeclare($this->name, false, true, false, false, false, $waitQueueArgs);
        }
    }


    public function publish(QueueMessage $message, \DateTimeImmutable $delayTo = null): PromiseInterface
    {
        if ($delayTo !== null) {
            throw new UnsupportedOperationException("Message specific delays are not supported on Delayed Queues");
        }

        return parent::publish($message);
    }


    /**
     * Returns the the number of messages waiting on delay (not the number in the "ready" queue)
     *
     * @noinspection SenselessProxyMethodInspection
     */
    // phpcs:ignore Generic.CodeAnalysis.UselessOverridingMethod
    public function getMessageCount(): ?int
    {
        return parent::getMessageCount();
    }

}
