<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface KafkaConsumerBuilderInterface
{

    const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    const OFFSET_END = RD_KAFKA_OFFSET_END;
    const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /**
     * @param string $broker
     * @return KafkaConsumerBuilderInterface
     */
    public function addBroker(string $broker): self;

    /**
     * @param string  $topicName
     * @param array   $partitions
     * @param integer $offset
     * @return KafkaConsumerBuilderInterface
     */
    public function addSubscription(string $topicName, array $partitions = [], int $offset = self::OFFSET_STORED): self;

    /**
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function addConfig(array $config): self;

    /**
     * @param integer $timeout
     * @return KafkaConsumerBuilderInterface
     */
    public function setTimeout(int $timeout): self;

    /**
     * @param string $consumerGroup
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): self;

    /**
     * @param string $consumerType
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerType(string $consumerType): self;

    /**
     * @param callable $errorCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setErrorCallback(callable $errorCallback): self;

    /**
     * @param callable $rebalanceCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setRebalanceCallback(callable $rebalanceCallback): self;

    /**
     * @return KafkaConsumerInterface
     */
    public function build(): KafkaConsumerInterface;
}
