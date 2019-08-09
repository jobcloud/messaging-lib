<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaMessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;

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
     * @return KafkaMessageInterface
     */
    public function consume(): MessageInterface;

    /**
     * @param KafkaMessageInterface|KafkaMessageInterface[] $messages
     * @return void
     */
    public function commit($messages): void;

    /**
     * @return KafkaConfiguration
     */
    public function getConfiguration(): KafkaConfiguration;

    /**
     * @param RdKafkaConsumerTopic $topic
     * @return RdKafkaMetadataTopic
     */
    public function getMetadataForTopic(RdKafkaConsumerTopic $topic): RdKafkaMetadataTopic;
}
