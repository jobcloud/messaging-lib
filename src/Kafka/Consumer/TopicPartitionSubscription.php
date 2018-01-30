<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

/**
 * This topic subscription needs an concrete subscription to (a subset of available) partitions by using
 * the @see self::addPartition method. Only the partitions explicitly added will be subscribed.
 */
class TopicPartitionSubscription implements TopicSubscriptionInterface
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
     * @param string $topicName
     */
    public function __construct(string $topicName)
    {
        $this->topicName  = $topicName;
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
    public function addPartition(int $partitionId, int $offset): self
    {
        $this->partitions[$partitionId] = $offset;

        return $this;
    }

    /**
     * @return int[]
     */
    public function getPartitions(): array
    {
        return $this->partitions;
    }
}
