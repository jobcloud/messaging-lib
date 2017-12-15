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
     * @var array
     */
    protected $topics;

    /**
     * AbstractKafkaConsumer constructor.
     * @param RdKafkaConsumer $consumer
     * @param array           $topics
     * @throws KafkaConsumerException
     */
    public function __construct(RdKafkaConsumer $consumer, array $topics)
    {
        $this->consumer = $consumer;

        if ([] !== $topics) {
            try {
                $this->consumer->subscribe($topics);
            } catch (RdKafkaException $e) {
                throw new KafkaConsumerException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
