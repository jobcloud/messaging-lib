<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\KafkaConfiguration;

interface KafkaConsumerInterface extends ConsumerInterface
{
    /**
     * Tries to subscribe to the given topics and returns a list of successfully subscribed topics
     * @return void
     */
    public function subscribe(): void;

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return void
     */
    public function unsubscribe(): void;

    /**
     * @return boolean
     */
    public function isSubscribed(): bool;

    /**
     * @return MessageInterface
     */
    public function consume(): MessageInterface;

    /**
     * @return array|TopicSubscriptionInterface[]
     */
    public function getTopicSubscriptions(): array;

    /**
     * @param MessageInterface|MessageInterface[] $messages
     * @return void
     */
    public function commit($messages): void;

    /**
     * @return KafkaConfiguration|null
     */
    public function getConfiguration(): ?KafkaConfiguration;
}
