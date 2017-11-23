<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;

interface KafkaConsumerFactoryInterface
{
    public function createConsumer(array $config): ConsumerInterface;
}
