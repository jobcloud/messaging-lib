<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;

interface KafkaConsumerBuilderInterface
{
    /**
     * @return ConsumerInterface
     */
    public function build(): ConsumerInterface;
}
