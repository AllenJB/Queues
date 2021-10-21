<?php
declare(strict_types=1);

namespace AllenJB\Queues\Rabbit;

use Bunny\AbstractClient;
use AllenJB\Queues\UnsupportedOperationException;

/**
 * Dirty cheat to get access to the options on a Bunny Client object
 */
class BunnyClientProxy extends AbstractClient
{

    /**
     * @var AbstractClient
     */
    protected $client;


    /** @noinspection MagicMethodsValidityInspection PhpMissingParentConstructorInspection */
    public function __construct(AbstractClient $client)
    {
        $this->client = $client;
    }


    public function disconnect($replyCode = 0, $replyText = "")
    {
        throw new UnsupportedOperationException("Unimplemented");
    }


    public function run($maxSeconds = null)
    {
        throw new UnsupportedOperationException("Unimplemented");
    }


    /**
     * @phpstan-ignore-next-line
     */
    protected function feedReadBuffer()
    {
        throw new UnsupportedOperationException("Unimplemented");
    }


    protected function flushWriteBuffer()
    {
        throw new UnsupportedOperationException("Unimplemented");
    }


    public function getHost(): ?string
    {
        return $this->client->options["host"];
    }


    public function getVHost(): ?string
    {
        return $this->client->options["vhost"];
    }


    public function getUsername(): ?string
    {
        return $this->client->options["user"];
    }


    public function getPassword(): ?string
    {
        return $this->client->options["password"];
    }

}
