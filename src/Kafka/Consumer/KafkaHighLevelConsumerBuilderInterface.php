<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\KafkaConsumer;

interface KafkaHighLevelConsumerBuilderInterface
{

    /**
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function setConfig(array $config): self;

    /**
     * @param string $consumerGroup
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): self;

    /**
     * @return KafkaConsumer
     */
    public function build(): KafkaConsumer;
}
