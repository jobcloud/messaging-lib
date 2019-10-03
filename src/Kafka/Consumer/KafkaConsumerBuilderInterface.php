<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Message\Decoder\DecoderInterface;

interface KafkaConsumerBuilderInterface
{

    public const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    public const OFFSET_END = RD_KAFKA_OFFSET_END;
    public const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /**
     * Adds a broker from which you want to consume
     *
     * @param string $broker
     * @return KafkaConsumerBuilderInterface
     */
    public function withAdditionalBroker(string $broker): self;

    /**
     * Add topic name(s) (and additionally partition(s) and offset(s)) to subscribe to
     *
     * @param string  $topicName
     * @param array   $partitions
     * @param integer $offset
     * @return KafkaConsumerBuilderInterface
     */
    public function withAdditionalSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): self;

    /**
     * Replaces all topic names previously configured with a topic and additionally partitions and an offset to
     * subscribe to
     *
     * @param string  $topicName
     * @param array   $partitions
     * @param integer $offset
     * @return KafkaConsumerBuilderInterface
     */
    public function withSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): self;

    /**
     * Add configuration settings, otherwise the kafka defaults apply
     *
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function withAdditionalConfig(array $config): self;

    /**
     * Set the timeout for all consumer actions
     *
     * @param integer $timeout
     * @return KafkaConsumerBuilderInterface
     */
    public function withTimeout(int $timeout): self;

    /**
     * Set the consumer group
     *
     * @param string $consumerGroup
     * @return KafkaConsumerBuilderInterface
     */
    public function withConsumerGroup(string $consumerGroup): self;

    /**
     * Set the consumer type, can be either CONSUMER_TYPE_LOW_LEVEL or CONSUMER_TYPE_HIGH_LEVEL
     *
     * @param string $consumerType
     * @return KafkaConsumerBuilderInterface
     */
    public function withConsumerType(string $consumerType): self;

    /**
     * Set a callback to be called on errors.
     * The default callback will throw an exception for every error
     *
     * @param callable $errorCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withErrorCallback(callable $errorCallback): self;

    /**
     * Set a callback to be called on consumer rebalance
     *
     * @param callable $rebalanceCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withRebalanceCallback(callable $rebalanceCallback): self;

    /**
     * Only applicable for the high level consumer
     * Callback that is going to be called when you call consume
     *
     * @param callable $consumeCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withConsumeCallback(callable $consumeCallback): self;

    /**
     * Set callback that is being called on offset commits
     *
     * @param callable $offsetCommitCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withOffsetCommitCallback(callable $offsetCommitCallback): self;

    /**
     * Lets you set a custom decoder for the consumed message
     *
     * @param DecoderInterface $decoder
     * @return KafkaConsumerBuilderInterface
     */
    public function withDecoder(DecoderInterface $decoder): self;

    /**
     * Returns your consumer instance
     *
     * @return KafkaConsumerInterface
     */
    public function build(): KafkaConsumerInterface;
}
