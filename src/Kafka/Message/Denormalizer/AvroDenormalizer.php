<?php

namespace Jobcloud\Messaging\Kafka\Message\Denormalizer;

use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;

class AvroDenormalizer implements DenormalizerInterface
{
    public function __construct(Registry $registry)
    {
    }

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     */
    public function denormalize(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {

        return $consumerMessage;
    }
}
