<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;

class KafkaHighLevelConsumer implements KafkaConsumerInterface
{

    /**
     * Tries to subscribe to the given topics and returns a list of successfully subscribed topics
     * @return void
     */
    public function subscribe(): void
    {
        // TODO: Implement subscribe() method.
    }

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return void
     */
    public function unsubscribe(): void
    {
        // TODO: Implement unsubscribe() method.
    }

    /**
     * @return boolean
     */
    public function isSubscribed(): bool
    {
        // TODO: Implement isSubscribed() method.
    }

    /**
     * @return MessageInterface
     */
    public function consume(): MessageInterface
    {
        // TODO: Implement consume() method.
    }

    /**
     * @return array|TopicSubscriptionInterface[]
     */
    public function getTopicSubscriptions(): array
    {
        // TODO: Implement getTopicSubscriptions() method.
    }

    /**
     * @param MessageInterface|MessageInterface[] $messages
     * @return void
     */
    public function commit($messages): void
    {
        // TODO: Implement commit() method.
    }

    /**
     * @return KafkaConfiguration
     */
    public function getConfiguration(): KafkaConfiguration
    {
        // TODO: Implement getConfiguration() method.
    }
}
