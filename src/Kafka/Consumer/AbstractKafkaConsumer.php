<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use RdKafka\KafkaConsumer;

abstract class AbstractKafkaConsumer implements ConsumerInterface
{

    /**
     * @var KafkaConsumer
     */
    protected $consumer;

    /**
     * @var array
     */
    protected $topics;

    /**
     * AbstractKafkaConsumer constructor.
     * @param KafkaConsumer $consumer
     * @param array         $topics
     * @throws \RdKafka\Exception
     */
    public function __construct(KafkaConsumer $consumer, array $topics)
    {
        $this->consumer = $consumer;

        if ([] !== $topics) {
            $this->consumer->subscribe($topics);
        }
    }
}
