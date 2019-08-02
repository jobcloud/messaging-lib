<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use RdKafka\Consumer as RdKafkaConsumer;
use RdKafka\ConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Metadata;
use RdKafka\Queue;

final class KafkaLowLevelConsumer extends AbstractKafkaConsumer
{

    /** @var array|ConsumerTopic[] */
    protected $topics = [];

    /** @var Queue */
    protected $queue;

    /**
     * @param RdKafkaConsumer    $consumer
     * @param KafkaConfiguration $kafkaConfiguration
     */
    public function __construct(RdKafkaConsumer $consumer, KafkaConfiguration $kafkaConfiguration)
    {
        $this->consumer = $consumer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->queue = $consumer->newQueue();
    }

    /**
     * @return MessageInterface
     * @throws KafkaConsumerConsumeException
     */
    public function consume(): MessageInterface
    {
        if (false === $this->subscribed) {
            throw new KafkaConsumerConsumeException('This consumer is currently not subscribed');
        }

        if (null === $rdKafkaMessage = $this->queue->consume($this->kafkaConfiguration->getTimeout())) {
            throw new KafkaConsumerConsumeException(
                rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                RD_KAFKA_RESP_ERR__TIMED_OUT
            );
        }

        if ($rdKafkaMessage->topic_name === null && RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        }

        $message = new Message(
            $rdKafkaMessage->key,
            $rdKafkaMessage->payload,
            $rdKafkaMessage->topic_name,
            $rdKafkaMessage->partition,
            $rdKafkaMessage->offset,
            $rdKafkaMessage->timestamp,
            $rdKafkaMessage->headers
        );

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        }

        return $message;
    }

    /**
     * Tries to subscribe to the given topics and returns a list of successfully subscribed topics
     * @return void
     * @throws KafkaConsumerSubscriptionException
     */
    public function subscribe(): void
    {
        if (true === $this->subscribed) {
            return;
        }

        $this->connectConsumerToBrokers();

        try {
            $topicSubscriptions = $this->kafkaConfiguration->getTopicSubscriptions();
            foreach ($topicSubscriptions as $topicSubscription) {
                $topicName = $topicSubscription->getTopicName();

                if (false === isset($this->topics[$topicName])) {
                    $this->topics[$topicName] = $topic = $this->consumer->newTopic(
                        $topicName,
                        $topicSubscription->getTopicConf()
                    );

                    //Check partition subscription, if not set, subscribe to all partitions
                    $this->verifyAndSetPartitionSubcriptions($topic, $topicSubscription);
                } else {
                    $topic = $this->topics[$topicName];
                }

                foreach ($topicSubscription->getPartitions() as $partitionId => $offset) {
                    $topic->consumeQueueStart($partitionId, $offset, $this->queue);
                }
            }

            $this->subscribed = true;
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param MessageInterface[]|MessageInterface $messages
     * @return void
     * @throws KafkaConsumerCommitException
     */
    public function commit($messages): void
    {
        $messages = is_array($messages) ? $messages : [$messages];

        foreach ($messages as $i => $message) {
            if (false === $message instanceof Message) {
                throw new KafkaConsumerCommitException(
                    sprintf('Provided message (index: %d) is not an instance of "%s"', $i, Message::class)
                );
            }

            $this->topics[$message->getTopicName()]->offsetStore(
                $message->getPartition(),
                $message->getOffset()
            );
        }
    }

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return void
     */
    public function unsubscribe(): void
    {
        if (false === $this->subscribed) {
            return;
        }

        $topicSubscriptions = $this->kafkaConfiguration->getTopicSubscriptions();

        /** @var TopicSubscription $topicSubscription */
        foreach ($topicSubscriptions as $topicSubscription) {
            foreach ($topicSubscription->getPartitions() as $partitionId => $offset) {
                $this->topics[$topicSubscription->getTopicName()]->consumeStop($partitionId);
            }
        }

        $this->subscribed = false;
    }

    /**
     * @param ConsumerTopic $topic
     * @return Metadata\Topic
     * @throws RdKafkaException
     */
    private function getMetadataForTopic(ConsumerTopic $topic): Metadata\Topic
    {
        return $this->consumer
            ->getMetadata(
                false,
                $topic,
                $this->kafkaConfiguration->getTimeout()
            )
            ->getTopics()
            ->current();
    }

    /**
     * @param ConsumerTopic     $topic
     * @param TopicSubscription $topicSubscription
     * @throws RdKafkaException
     * @return void
     */
    private function verifyAndSetPartitionSubcriptions(ConsumerTopic $topic, TopicSubscription $topicSubscription): void
    {
        if ([] === $topicSubscription->getPartitions()) {
            $topicMetadata = $this->getMetadataForTopic($topic);

            foreach ($topicMetadata->getPartitions() as $partition) {
                $topicSubscription->addPartition(
                    $partition->getId(),
                    $topicSubscription->getDefaultOffset()
                );
            }
        }
    }
}
