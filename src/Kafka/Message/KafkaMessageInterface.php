<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

use Jobcloud\Messaging\Consumer\MessageInterface;

interface KafkaMessageInterface extends MessageInterface
{
    /**
     * @return string|null
     */
    public function getKey(): ?string;

    /**
     * @return string
     */
    public function getTopicName(): string;

    /**
     * @return integer
     */
    public function getOffset(): int;

    /**
     * @return integer
     */
    public function getPartition(): int;

    /**
     * @return int
     */
    public function getTimestamp(): int;

    /**
     * @return array|null
     */
    public function getHeaders(): ?array;
}
