<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

final class KafkaMessage implements KafkaMessageInterface
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
     * @param string  $topicName
     * @param integer $partition
     */
    private function __construct(string $topicName, int $partition)
    {
        $this->topicName    = $topicName;
        $this->partition    = $partition;
    }

    /**
     * @param string  $topicName
     * @param integer $partition
     * @return KafkaMessageInterface
     */
    public static function create(string $topicName, int $partition): KafkaMessageInterface
    {
        return new self($topicName, $partition);
    }

    /**
     * @param string|null $key
     * @return KafkaMessageInterface
     */
    public function withKey(?string $key): KafkaMessageInterface
    {
        $new = clone $this;

        $new->key = $key;

        return $new;
    }

    /**
     * @param string|null $body
     * @return KafkaMessageInterface
     */
    public function withBody(?string $body): KafkaMessageInterface
    {
        $new = clone $this;

        $new->body = $body;

        return $new;
    }

    /**
     * @param integer $offset
     * @return KafkaMessageInterface
     */
    public function withOffset(int $offset): KafkaMessageInterface
    {
        $new = clone $this;

        $new->offset = $offset;

        return $new;
    }

    /**
     * @param integer $timestamp
     * @return KafkaMessageInterface
     */
    public function withTimestamp(int $timestamp): KafkaMessageInterface
    {
        $new = clone $this;

        $new->timestamp = $timestamp;

        return $new;
    }

    /**
     * @param array|null $headers
     * @return KafkaMessageInterface
     */
    public function withHeaders(?array $headers): KafkaMessageInterface
    {
        $new = clone $this;

        $new->headers = $headers;

        return $new;
    }

    /**
     * @param string         $key
     * @param string|integer $value
     * @return KafkaMessageInterface
     */
    public function withHeader(string $key, $value): KafkaMessageInterface
    {
        $new = clone $this;

        $new->headers[$key] = $value;

        return $new;
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
     * @return integer|null
     */
    public function getOffset(): ?int
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
     * @return integer
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
