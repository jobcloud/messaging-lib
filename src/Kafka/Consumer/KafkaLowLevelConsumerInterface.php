<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\TopicPartition;

interface KafkaLowLevelConsumerInterface extends KafkaConsumerInterface
{
    /**
     * @param array|TopicPartition[] $topicPartitions
     * @param integer                $timeout
     * @return array
     */
    public function offsetsForTimes(array $topicPartitions, int $timeout): array;

    /**
     * @param string  $topic
     * @param integer $partition
     * @param integer $lowOffset
     * @param integer $highOffset
     * @param integer $timeout
     * @return void
     */
    public function getBrokerHighLowOffsets(
        string $topic,
        int $partition,
        int &$lowOffset,
        int &$highOffset,
        int $timeout
    ): void;
}
