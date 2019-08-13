<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface KafkaLowLevelConsumerInterface extends KafkaConsumerInterface
{

    /**
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getFirstOffsetForTopicPartition(string $topic, int $partition, int $timeout): int;

    /**
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getLastOffsetForTopicPartition(string $topic, int $partition, int $timeout): int;
}
