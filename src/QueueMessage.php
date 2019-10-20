<?php
declare(strict_types=1);

namespace AllenJB\Queues;

class QueueMessage
{

    protected $messageId = null;

    protected $attempts = 0;

    protected $data;

    protected $replyTo = null;

    protected $correlationId = null;

    protected $original = null;


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


    public function withOriginal($originalMsg): QueueMessage
    {
        $msg = clone $this;
        $msg->original = $originalMsg;
        return $msg;
    }


    public function getOriginal()
    {
        return $this->original;
    }

}
