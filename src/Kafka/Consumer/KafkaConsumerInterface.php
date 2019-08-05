<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use RdKafka\ConsumerTopic;
use RdKafka\Metadata;

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
     * @param MessageInterface|MessageInterface[] $messages
     * @return void
     */
    public function commit($messages): void;

    /**
     * @return KafkaConfiguration
     */
    public function getConfiguration(): KafkaConfiguration;

    /**
     * @param ConsumerTopic $topic
     * @return Metadata\Topic
     */
    public function getMetadataForTopic(ConsumerTopic $topic): Metadata\Topic;
}
