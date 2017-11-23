<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Kafka\Consumer\KafkaConsumerFactoryInterface;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Helper\ConfigTrait;
use \RdKafka\Consumer as RdKafkaConsumer;

final class KafkaConsumerFactory implements KafkaConsumerFactoryInterface
{

    use ConfigTrait;

    public function createConsumer(array $config): ConsumerInterface
    {
        $kafkaConsumer = new RdKafkaConsumer($config);

        return new Consumer($kafkaConsumer, );
    }
}
