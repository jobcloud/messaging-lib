<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use RdKafka\KafkaConsumer;

abstract class AbstractKafkaConsumer implements ConsumerInterface
{

    protected $consumer;

    protected $topics;

    /**
     * AbstractKafkaConsumer constructor.
     * @param KafkaConsumer $consumer
     * @param array         $topics
     */
    public function __construct(KafkaConsumer $consumer, array $topics)
    {
        $this->consumer = $consumer;

        if (!empty($topics)) {
            $this->subscribe($topics);
        }
    }

    /**
     * @param array $topics
     * @return void
     */
    public function subscribe(array $topics)
    {
        $this->consumer->subscribe($topics);
    }
}
