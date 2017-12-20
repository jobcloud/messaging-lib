<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface KafkaConsumerBuilderInterface
{
    /**
     * @return KafkaConsumer
     */
    public function build(): KafkaConsumer;
}
