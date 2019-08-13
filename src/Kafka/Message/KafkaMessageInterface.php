<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

use Jobcloud\Messaging\Consumer\MessageInterface;

interface KafkaMessageInterface extends MessageInterface
{
    /**
     * @param string  $topicName
     * @param integer $partition
     * @return KafkaMessageInterface
     */
    public static function create(string $topicName, int $partition): KafkaMessageInterface;

    /**
     * @param string|null $key
     * @return KafkaMessageInterface
     */
    public function withKey(?string $key): KafkaMessageInterface;

    /**
     * @param string|null $body
     * @return KafkaMessageInterface
     */
    public function withBody(?string $body): KafkaMessageInterface;

    /**
     * @param integer $offset
     * @return KafkaMessageInterface
     */
    public function withOffset(int $offset): KafkaMessageInterface;

    /**
     * @param integer $timestamp
     * @return KafkaMessageInterface
     */
    public function withTimestamp(int $timestamp): KafkaMessageInterface;

    /**
     * @param array $headers
     * @return KafkaMessageInterface
     */
    public function withHeaders(array $headers): KafkaMessageInterface;

    /**
     * @return string|null
     */
    public function getKey(): ?string;

    /**
     * @return string
     */
    public function getTopicName(): string;

    /**
     * @return integer|null
     */
    public function getOffset(): ?int;

    /**
     * @return integer
     */
    public function getPartition(): int;

    /**
     * @return integer
     */
    public function getTimestamp(): int;

    /**
     * @return array|null
     */
    public function getHeaders(): ?array;
}
