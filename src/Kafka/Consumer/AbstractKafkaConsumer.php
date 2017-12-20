<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Exception as RdKafkaException;

abstract class AbstractKafkaConsumer implements ConsumerInterface
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
     * AbstractKafkaConsumer constructor.
     * @param RdKafkaConsumer $consumer
     * @param array           $topics
     */
    public function __construct(RdKafkaConsumer $consumer, array $topics)
    {
        $this->consumer = $consumer;
        $this->topics = $topics;
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
