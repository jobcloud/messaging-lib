<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Exception as RdKafkaException;

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
     * @param int             $timeout
     */
    public function __construct(RdKafkaConsumer $consumer, array $topics, int $timeout)
    {
        $this->consumer = $consumer;
        $this->topics = $topics;
        $this->timeout = $timeout;
    }

    /**
     * @return MessageInterface|Message
     * @throws KafkaConsumerException
     */
    public function consume(): MessageInterface
    {
        try {
            $message = $this->consumer->consume($this->timeout);

            if (null !== $message->err
                && RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err
                && RD_KAFKA_RESP_ERR__PARTITION_EOF !== $message->err
            ) {
                throw new KafkaConsumerException(
                    sprintf(KafkaConsumerException::CONSUMPTION_EXCEPTION_MESSAGE, $message->errstr()),
                    $message->err
                );
            }

            return new Message(
                $message->payload,
                $message->topic_name,
                $message->partition,
                $message->offset,
                $message->err,
                $message->errstr()
            );
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerException($e->getMessage(), $e->getCode(), $e);
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
     * @throws KafkaConsumerException
     */
    public function subscribe(): array
    {
        try {
            $this->consumer->subscribe($this->topics);

            return $this->getSubscription();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return array List of successfully unsubscribed topics
     * @throws KafkaConsumerException
     */
    public function unsubscribe(): array
    {
        if ([] === $this->topics) {
            return [];
        }

        try {
            $this->consumer->unsubscribe();

            return $this->getSubscription();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerException($e->getMessage(), $e->getCode(), $e);
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

    /**
     * @throws KafkaConsumerException
     */
    public function __destruct()
    {
        $this->unsubscribe();
    }
}
