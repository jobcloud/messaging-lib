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
        $this->consumer = $this->setConsumer($consumer);

        if (!empty($topics)) {
            $this->subscribe($topics);
        }
    }

    /**
     * @param array $topics
     */
    public function subscribe(array $topics)
    {
        $this->getConsumer()->subscribe($topics);
    }

    /**
     * @param KafkaConsumer $consumer
     */
    public function setConsumer(KafkaConsumer $consumer)
    {
        $this->consumer = $consumer;
    }

    /**
     * @return KafkaConsumer
     */
    public function getConsumer(): KafkaConsumer
    {
        return $this->consumer;
    }
}
