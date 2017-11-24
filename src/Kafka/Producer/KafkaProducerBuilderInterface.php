<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;

interface KafkaProducerBuilderInterface
{
    /**
     * @return ProducerInterface
     */
    public function build(): ProducerInterface;
}
