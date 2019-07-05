<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;

class Message implements MessageInterface
{

    /**
     * @var string|null
     */
    private $key;

    /**
     * @var string|null
     */
    private $body;

    /**
     * @var string
     */
    private $topicName;
    
    /**
     * @var int
     */
    private $offset;

    /**
     * @var int
     */
    private $partition;

    /**
     * @var int
     */
    private $timestamp;

    /**
     * @var array|null
     */
    private $headers;

    /**
     * @param null|string $key
     * @param null|string $body
     * @param string      $topicName
     * @param integer     $partition
     * @param integer     $offset
     * @param integer     $timestamp
     * @param null|array  $headers
     */
    public function __construct(
        ?string $key,
        ?string $body,
        string $topicName,
        int $partition,
        int $offset,
        int $timestamp,
        ?array $headers
    ) {
        $this->key          = $key;
        $this->body         = $body;
        $this->topicName    = $topicName;
        $this->partition    = $partition;
        $this->offset       = $offset;
        $this->timestamp    = $timestamp;
        $this->headers      = $headers;
    }

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
    public function getOffset(): int
    {
        return $this->offset;
    }

    /**
     * @return integer
     */
    public function getPartition(): int
    {
        return $this->partition;
    }

    /**
     * @return int
     */
    public function getTimestamp(): int
    {
        return $this->timestamp;
    }

    /**
     * @return array|null
     */
    public function getHeaders(): ?array
    {
        return $this->headers;
    }
}
