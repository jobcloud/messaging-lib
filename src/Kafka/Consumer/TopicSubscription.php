<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

/**
 * This topic subscription needs an concrete subscription to (a subset of available) partitions by using
 * the @see self::addPartition method. Only the partitions explicitly added will be subscribed.
 */
class TopicSubscription implements TopicSubscriptionInterface
{

    /**
     * @var string
     */
    private $topicName;

    /**
     * @var int[]
     */
    private $partitions = [];

    /**
     * @var int
     */
    private $offset;

    /**
     * @param string $topicName
     * @param int    $offset
     */
    public function __construct(string $topicName, int $offset = RD_KAFKA_OFFSET_STORED)
    {
        $this->topicName  = $topicName;
        $this->offset = $offset;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @param int $partitionId
     * @param int $offset
     * @return self
     */
    public function addPartition(int $partitionId, int $offset = null): self
    {
        $this->partitions[$partitionId] = $offset !== null ? $offset : $this->offset;

        return $this;
    }

    /**
     * @return int[]
     */
    public function getPartitions(): array
    {
        return $this->partitions;
    }

    /**
     * @return int
     */
    public function getOffset(): int
    {
        return $this->offset;
    }
}
