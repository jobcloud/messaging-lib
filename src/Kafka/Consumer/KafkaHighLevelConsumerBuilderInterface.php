<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\KafkaConsumer;

interface KafkaHighLevelConsumerBuilderInterface
{

    /**
     * @param string $broker
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function addBroker(string $broker): self;

    /**
     * @param array $config
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConfig(array $config): self;

    /**
     * @param TopicSubscriptionInterface $topicSubscription
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function addSubscription(TopicSubscriptionInterface $topicSubscription): self;

    /**
     * @param integer $timeout
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setTimeout(int $timeout): self;

    /**
     * @param string $consumerGroup
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): self;

    /**
     * @return KafkaConsumer
     */
    public function build(): KafkaConsumer;
}
