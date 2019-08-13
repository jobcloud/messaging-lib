<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface KafkaLowLevelConsumerInterface extends KafkaConsumerInterface
{

    /**
     * Queries the broker for the first offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getFirstOffsetForTopicPartition(string $topic, int $partition, int $timeout): int;

    /**
     * Queries the broker for the last offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getLastOffsetForTopicPartition(string $topic, int $partition, int $timeout): int;
}
