<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use RdKafka\KafkaConsumer;

final class KafkaHighLevelConsumerBuilder implements KafkaHighLevelConsumerBuilderInterface
{

    use KafkaConfigTrait;

    /**
     * @var array
     */
    private $config = [];

    /**
     * @var string
     */
    private $consumerGroup = 'default';

    /**
     * @return KafkaHighLevelConsumerBuilder
     */
    public static function create(): self
    {
        return new self();
    }

    /**
     * @param array $config
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConfig(array $config): KafkaHighLevelConsumerBuilderInterface
    {
        $this->config += $config;

        return $this;
    }

    /**
     * @param string $consumerGroup
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): KafkaHighLevelConsumerBuilderInterface
    {
        $this->consumerGroup = $consumerGroup;

        return $this;
    }

    /**
     * @return KafkaConsumer
     */
    public function build(): KafkaConsumer
    {
        $this->config['group.id'] = $this->consumerGroup;

        $kafkaConfig = $this->createKafkaConfig($this->config);

        return new KafkaConsumer($kafkaConfig);
    }
}
