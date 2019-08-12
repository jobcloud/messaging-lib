<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

/**
 * This topic subscription needs an concrete subscription to (a subset of available) partitions by using
 * the @see self::addPartition method. Only the partitions explicitly added will be subscribed.
 */
final class TopicSubscription implements TopicSubscriptionInterface
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
     * @param string       $topicName
     * @param array        $partitions
     * @param integer|null $offset
     */
    public function __construct(
        string $topicName,
        array $partitions = [],
        ?int $offset = RD_KAFKA_OFFSET_STORED
    ) {
        $this->topicName = $topicName;
        $this->partitions = $partitions;
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
     * @param array $partitions
     * @return void
     */
    public function setPartitions(array $partitions): void
    {
        $this->partitions = $partitions;
    }

    /**
     * @return int[]
     */
    public function getPartitions(): array
    {
        return $this->partitions;
    }

    /**
     * @return integer
     */
    public function getOffset(): ?int
    {
        return $this->offset;
    }
}
