<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Exception as RdKafkaException;
use RdKafka\TopicPartition;

final class KafkaConsumer implements ConsumerInterface
{

    /**
     * @var RdKafkaConsumer
     */
    protected $consumer;

    /**
     * @var array|string[]
     */
    protected $topics;

    /**
     * @var integer
     */
    protected $timeout;

    /**
     * AbstractKafkaConsumer constructor.
     * @param RdKafkaConsumer $consumer
     * @param array           $topics
     * @param integer         $timeout
     */
    public function __construct(RdKafkaConsumer $consumer, array $topics, int $timeout)
    {
        $this->consumer = $consumer;
        $this->topics = $topics;
        $this->timeout = $timeout;
    }

    /**
     * @return MessageInterface|Message|null
     * @throws KafkaConsumerConsumeException
     */
    public function consume(): ?MessageInterface
    {
        try {
            $rdKafkaMessage = $this->consumer->consume($this->timeout);

            if (RD_KAFKA_RESP_ERR__TIMED_OUT === $rdKafkaMessage->err
                || RD_KAFKA_RESP_ERR__PARTITION_EOF === $rdKafkaMessage->err
            ) {
                return null;
            }

            if (null === $rdKafkaMessage->topic_name && RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
                throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
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
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerConsumeException($e->getMessage(), $e->getCode(), null, $e);
        }
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
     * @return array List of successfully subscribed topics
     * @throws KafkaConsumerSubscriptionException
     */
    public function subscribe(): array
    {
        try {
            $this->consumer->subscribe($this->topics);

            return $this->getSubscription();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param Message[]|Message|null $messages
     * @return void
     * @throws KafkaConsumerCommitException
     */
    public function commit($messages = null): void
    {
        try {
            if (null === $messages) {
                $this->consumer->commit();
                return;
            }

            $messages = is_array($messages) ? $messages : [$messages];
            $offsets = [];

            foreach ($messages as $i => $message) {
                if (false === $message instanceof Message) {
                    throw new KafkaConsumerCommitException(
                        sprintf('Provided message (offset: %d) is not an instance of "%s"', $i, Message::class)
                    );
                }

                $offsets[] = new TopicPartition(
                    $message->getTopicName(),
                    $message->getPartition(),
                    $message->getOffset()
                );
            }

            $this->consumer->commit($offsets);
        } catch (RdKafkaException $e) {
            if (RD_KAFKA_RESP_ERR__NO_OFFSET === $e->getCode()) {
                return;
            }

            throw new KafkaConsumerCommitException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return array List of successfully unsubscribed topics
     * @throws KafkaConsumerSubscriptionException
     */
    public function unsubscribe(): array
    {
        try {
            $this->consumer->unsubscribe();

            return $this->getSubscription();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Returns all the topics to which this consumer is currently subscribed
     * @return array
     */
    protected function getSubscription(): array
    {
        return $this->consumer->getSubscription();
    }
}
