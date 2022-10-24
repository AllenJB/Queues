<?php
declare(strict_types=1);

namespace AllenJB\Queues;

class QueueMessage
{

    protected ?int $messageId = null;

    protected int $attempts = 0;

    /**
     * @var mixed
     */
    protected $data;

    protected ?string $replyTo = null;

    protected ?string $correlationId = null;

    /**
     * @var null|mixed
     */
    protected $original = null;


    /**
     * @param mixed $data
     */
    public function __construct($data)
    {
        $this->data = $data;
    }


    public function withAttempts(int $attempts): QueueMessage
    {
        $msg = clone $this;
        $msg->attempts = $attempts;
        return $msg;
    }


    public function withId(int $id): QueueMessage
    {
        $msg = clone $this;
        $msg->messageId = $id;
        return $msg;
    }


    public function getAttempts(): int
    {
        return $this->attempts;
    }


    public function getId(): ?int
    {
        return $this->messageId;
    }


    /**
     * @return mixed
     */
    public function getData()
    {
        return $this->data;
    }


    public function withReplyTo(?string $queueName): QueueMessage
    {
        $msg = clone $this;
        $msg->replyTo = $queueName;
        return $msg;
    }


    public function getReplyTo(): ?string
    {
        return $this->replyTo;
    }


    public function withCorrelationId(?string $corrId): QueueMessage
    {
        $msg = clone $this;
        $msg->correlationId = $corrId;
        return $msg;
    }


    public function getCorrelationId(): ?string
    {
        return $this->correlationId;
    }


    /**
     * Attach the original message this one was generated from (not persisted)
     * @param mixed $originalMsg
     */
    public function withOriginal($originalMsg): QueueMessage
    {
        $msg = clone $this;
        $msg->original = $originalMsg;
        return $msg;
    }


    /**
     * Retrieve the original message this one was generated from
     * @return mixed|null
     */
    public function getOriginal()
    {
        return $this->original;
    }

}
