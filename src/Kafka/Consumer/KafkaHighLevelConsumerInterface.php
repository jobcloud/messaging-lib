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
     * @return void
     */
    public function assign(): void;

    /**
     * @return void
     */
    public function commitAsync(): void;

    /**
     * @return array|TopicPartition[]
     */
    public function getAssignment(): array;

    /**
     * @return array|TopicPartition[]
     */
    public function getCommittedOffsets(): array;
}
