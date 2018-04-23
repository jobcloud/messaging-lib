<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\TopicConf;

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
    private $defaultOffset;

    /**
     * @var TopicConf
     */
    private $topicConf;

    /**
     * @param string  $topicName
     * @param integer $defaultOffset
     * @param integer $offsetCommitInterval
     */
    public function __construct(
        string $topicName,
        int $defaultOffset = RD_KAFKA_OFFSET_STORED,
        int $offsetCommitInterval = 1000
    ) {
        $this->topicName  = $topicName;
        $this->defaultOffset = $defaultOffset;
        $this->topicConf = new TopicConf();
        $this->topicConf->set('auto.commit.enable', 'false');
        $this->topicConf->set('auto.commit.interval.ms', strval($offsetCommitInterval));
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @param integer $partitionId
     * @param integer $offset
     * @return self
     */
    public function addPartition(int $partitionId, int $offset = null): self
    {
        $this->partitions[$partitionId] = $offset !== null ? $offset : $this->defaultOffset;

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
     * @return integer
     */
    public function getDefaultOffset(): int
    {
        return $this->defaultOffset;
    }

    /**
     * @return TopicConf
     */
    public function getTopicConf(): TopicConf
    {
        return $this->topicConf;
    }
}
