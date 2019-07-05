<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use RdKafka\Consumer as RdKafkaConsumer;
use RdKafka\ConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Metadata;
use RdKafka\Queue;

final class KafkaConsumer implements KafkaConsumerInterface
{

    const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    const OFFSET_END = RD_KAFKA_OFFSET_END;
    const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /**
     * @var RdKafkaConsumer
     */
    protected $consumer;

    /**
     * @var array|ConsumerTopic[]
     */
    protected $topics = [];

    /**
     * @var array|TopicSubscriptionInterface[]
     */
    protected $topicSubscriptions;

    /**
     * @var boolean
     */
    protected $subscribed = false;

    /**
     * @var integer
     */
    protected $timeout;

    /**
     * @var Queue
     */
    protected $queue;

    /**
     * @var array
     */
    protected $brokers;

    /**
     * @var boolean
     */
    protected $isConnected = false;

    /**
     * @param RdKafkaConsumer $consumer
     * @param array           $brokers
     * @param array           $topicSubscriptions
     * @param integer         $timeout
     */
    public function __construct(RdKafkaConsumer $consumer, array $brokers, array $topicSubscriptions, int $timeout)
    {
        $this->consumer = $consumer;
        $this->brokers = $brokers;
        $this->timeout = $timeout;
        $this->queue = $consumer->newQueue();
        $this->topicSubscriptions = $topicSubscriptions;
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

        if (null === $rdKafkaMessage = $this->queue->consume($this->timeout)) {
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
            $rdKafkaMessage->headers
        );

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        }

        return $message;
    }

    /**
     * @return array|TopicSubscriptionInterface[]
     */
    public function getTopicSubscriptions(): array
    {
        return $this->topicSubscriptions;
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
            foreach ($this->topicSubscriptions as $topicSubscription) {
                $topicName = $topicSubscription->getTopicName();

                if (false === isset($this->topics[$topicName])) {
                    $this->topics[$topicName] = $topic = $this->consumer->newTopic(
                        $topicName,
                        $topicSubscription->getTopicConf()
                    );

                    // Convert simple TopicSubscription to TopicPartitionSubscription
                    if ([] === $topicSubscription->getPartitions()) {
                        $topicMetadata = $this->getMetadataForTopic($topic);

                        foreach ($topicMetadata->getPartitions() as $partition) {
                            $topicSubscription->addPartition(
                                $partition->getId(),
                                $topicSubscription->getDefaultOffset()
                            );
                        }
                    }
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

        foreach ($this->topicSubscriptions as $topicSubscription) {
            foreach ($topicSubscription->getPartitions() as $partitionId => $offset) {
                $this->topics[$topicSubscription->getTopicName()]->consumeStop($partitionId);
            }
        }

        $this->subscribed = false;
    }

    /**
     * @return boolean
     */
    public function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    /**
     * @param ConsumerTopic $topic
     * @return Metadata\Topic
     * @throws RdKafkaException
     */
    private function getMetadataForTopic(ConsumerTopic $topic): Metadata\Topic
    {
        return $this->consumer->getMetadata(false, $topic, $this->timeout)->getTopics()->current();
    }

    /**
     * @return void
     */
    private function connectConsumerToBrokers(): void
    {
        if (true === $this->isConnected) {
            return;
        }

        $this->consumer->addBrokers(implode(',', $this->brokers));
        $this->isConnected = true;
    }
}
