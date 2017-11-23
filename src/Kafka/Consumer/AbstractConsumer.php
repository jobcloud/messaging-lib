<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use RdKafka\KafkaConsumer;

abstract class AbstractConsumer implements ConsumerInterface
{

    protected $consumer;

    protected $config;

    protected $topics;

    public function __construct(array $brokerList, array $topics, string $consumerGroup, array $config)
    {
        $config['groupId'] = $consumerGroup;
        $config['metadata.broker.list'] = implode(',', $brokerList);
        $this->consumer = new KafkaConsumer($this->config);
        $this->topics = $topics;
    }

    public function subscribe(array $topics)
    {
        $this->consumer->subscribe($topics);
    }
}
