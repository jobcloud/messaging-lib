<?php


namespace Jobcloud\Kafka\Consumer;


use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Jobcloud\Kafka\Helper\ConfigTrait;

abstract class AbstractConsumer implements ConsumerInterface
{

    use ConfigTrait;

    protected $consumer;

    protected $config;

    protected $topics;

    public function __construct(array $brokerList, array $topics, string $consumerGroup, array $config = [])
    {
        $config['groupId'] = $consumerGroup;
        $config['metadata.broker.list'] = implode(',', $brokerList);
        $this->config = $this->getConfig($config);
        $this->consumer = new KafkaConsumer($this->config);
        $this->topics = $topics;
    }

    public function subscribe(array $topics)
    {
        $this->consumer->subscribe($topics);
    }
}
