<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

abstract class AbstractKafkaMessage implements KafkaMessageInterface
{

    /**
     * @var string|null
     */
    protected $key;

    /**
     * @var string|null
     */
    protected $body;

    /**
     * @var string
     */
    protected $topicName;

    /**
     * @var int
     */
    protected $partition;

    /**
     * @var array|null
     */
    protected $headers;

    /**
     * @return string|null
     */
    public function getKey(): ?string
    {
        return $this->key;
    }

    /**
     * @return null|string
     */
    public function getBody(): ?string
    {
        return $this->body;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @return integer
     */
    public function getPartition(): int
    {
        return $this->partition;
    }

    /**
     * @return array|null
     */
    public function getHeaders(): ?array
    {
        return $this->headers;
    }
}
