<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface KafkaConsumerBuilderInterface
{
    /**
     * @param string $broker
     * @return KafkaConsumerBuilderInterface
     */
    public function addBroker(string $broker): self;

    /**
     * @param TopicSubscriptionInterface $topicSubscription
     * @return KafkaConsumerBuilderInterface
     */
    public function addSubscription(TopicSubscriptionInterface $topicSubscription): self;

    /**
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function setConfig(array $config): self;

    /**
     * @param integer $timeout
     * @return KafkaConsumerBuilderInterface
     */
    public function setTimeout(int $timeout): self;

    /**
     * @param string $consumerGroupBase
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerGroupBase(string $consumerGroupBase): self;

    /**
     * @param string $consumerGroup
     * @param bool   $isSuffixOnly
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup, bool $isSuffixOnly = false): self;

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
