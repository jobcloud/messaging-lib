<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;

abstract class AbstractKafkaConsumer implements KafkaConsumerInterface
{

    /** @var int */
    const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    /** @var int */
    const OFFSET_END = RD_KAFKA_OFFSET_END;
    /** @var int */
    const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /** @var KafkaConfiguration */
    protected $kafkaConfiguration;

    /** @var boolean */
    protected $subscribed = false;

    /** @var boolean */
    protected $isConnected = false;

    /** @var RdKafkaLowLevelConsumer|RdKafkaHighLevelConsumer */
    protected $consumer;

    /**
     * @return array|TopicSubscriptionInterface[]
     */
    public function getTopicSubscriptions(): array
    {
        return $this->kafkaConfiguration->getTopicSubscriptions();
    }

    /**
     * @return boolean
     */
    public function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    /**
     * @return KafkaConfiguration
     */
    public function getConfiguration(): KafkaConfiguration
    {
        return $this->kafkaConfiguration;
    }

    /**
     * @return void
     */
    protected function connectConsumerToBrokers(): void
    {
        if (true === $this->isConnected) {
            return;
        }

        $this->consumer->addBrokers(implode(',', $this->kafkaConfiguration->getBrokers()));
        $this->isConnected = true;
    }
}
