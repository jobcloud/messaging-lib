<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Denormalizer;

use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;

interface DenormalizerInterface
{
    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     */
    public function denormalize(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface;
}
