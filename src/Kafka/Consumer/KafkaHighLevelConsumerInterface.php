<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Message\KafkaMessageInterface;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

interface KafkaHighLevelConsumerInterface extends KafkaConsumerInterface
{
    /**
     * @param array $topicPartitions
     * @return void
     */
    public function assign(array $topicPartitions): void;

    /**
     * @param KafkaMessageInterface|KafkaMessageInterface[] $messages
     * @return void
     */
    public function commitAsync($messages): void;

    /**
     * @return array|RdKafkaTopicPartition[]
     */
    public function getAssignment(): array;

    /**
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeout
     * @return array|RdKafkaTopicPartition[]
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeout): array;

    /**
     * @param array $topicPartitions
     * @return array
     */
    public function getOffsetPositions(array $topicPartitions): array;
}
