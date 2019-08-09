<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\TopicConf as RdKafkaTopicConf;

interface TopicSubscriptionInterface
{

    /**
     * @param integer      $partitionId
     * @param integer|null $offset
     * @return TopicSubscriptionInterface
     */
    public function addPartition(int $partitionId, int $offset = null): self;

    /**
     * @return string
     */
    public function getTopicName(): string;

    /**
     * @return array
     */
    public function getPartitions(): array;

    /**
     * @return integer
     */
    public function getDefaultOffset(): int;

    /**
     * @return RdKafkaTopicConf
     */
    public function getTopicConf(): RdKafkaTopicConf;
}
