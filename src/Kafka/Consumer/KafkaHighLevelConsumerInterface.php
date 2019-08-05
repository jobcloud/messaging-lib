<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use RdKafka\TopicPartition;

interface KafkaHighLevelConsumerInterface extends KafkaConsumerInterface
{
    /**
     * @param array $topicPartitions
     * @return void
     */
    public function assign(array $topicPartitions): void;

    /**
     * @param MessageInterface|MessageInterface[] $messages
     * @return void
     */
    public function commitAsync($messages): void;

    /**
     * @return array|TopicPartition[]
     */
    public function getAssignment(): array;

    /**
     * @param array|TopicPartition[] $topicPartitions
     * @param integer                $timeout
     * @return array|TopicPartition[]
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeout): array;
}