<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerException;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use RdKafka\Consumer as RdKafkaConsumer;
use RdKafka\ConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Metadata;
use RdKafka\Queue;

final class KafkaConsumer implements ConsumerInterface
{

    const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    const OFFSET_END = RD_KAFKA_OFFSET_END;
    const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /**
     * @var RdKafkaConsumer
     */
    protected $consumer;

    /**
     * @var array|string[]
     */
    protected $topics = [];

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
     * AbstractKafkaConsumer constructor.
     * @param RdKafkaConsumer $consumer
     * @param array           $topics
     * @param integer         $timeout
     * @throws RdKafkaException
     */
    public function __construct(RdKafkaConsumer $consumer, array $topics, int $timeout)
    {
        $this->consumer = $consumer;
        $this->timeout = $timeout;
        $this->queue = $consumer->newQueue();

        foreach ($topics as $topicName => $topicInfo) {
            $topic = $this->consumer->newTopic($topicName);

            if ([] === $partitions = $topicInfo['partitions']) {
                $topicMetadata = $this->getMetadataForTopic($topic);

                foreach ($topicMetadata->getPartitions() as $partition) {
                    $partitions[$partition->getId()] = $topicInfo['offset'];
                }
            }

            $this->topics[$topicName] = [
                'partitions' => $partitions,
                'topic' => $topic
            ];
        }
    }

    /**
     * @return MessageInterface|Message|null
     * @throws ConsumerException
     */
    public function consume(): ?MessageInterface
    {
        if (false === $this->subscribed) {
            throw new KafkaConsumerConsumeException('This consumer has not subscribed to any topics');
        }

        if (null === $rdKafkaMessage = $this->queue->consume($this->timeout)) {
            throw new KafkaConsumerConsumeException(rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT), RD_KAFKA_RESP_ERR__TIMED_OUT);
        }

        if ($rdKafkaMessage->topic_name === null && RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->err . ': ' . $rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        }

        $message = new Message(
            $rdKafkaMessage->payload,
            $rdKafkaMessage->topic_name,
            $rdKafkaMessage->partition,
            $rdKafkaMessage->offset
        );

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        }

        return $message;
    }

    /**
     * @return array|string[]
     */
    public function getTopics(): array
    {
        return $this->topics;
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

        try {
            foreach ($this->topics as $topicName => $topicInfo) {
                foreach ($topicInfo['partitions'] as $partitionId => $offset) {
                    $topicInfo['topic']->consumeQueueStart($partitionId, $offset, $this->queue);
                }
            }

            $this->subscribed = true;
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param Message[]|Message $messages
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

            $this->topics[$message->getTopicName()]['topic']->offsetStore(
                $message->getPartition(), $message->getOffset()
            );
        }
    }

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return void
     * @throws KafkaConsumerSubscriptionException
     */
    public function unsubscribe(): void
    {
        if (false === $this->subscribed) {
            return;
        }

        try {
            foreach ($this->topics as $topicName => $topicInfo) {
                foreach ($topicInfo['partitions'] as $partitionId => $offset) {
                    $topicInfo['topic']->consumeStop($partitionId);
                }
            }
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

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
}
