<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Normalizer;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;

class NullNormalizer implements NormalizerInterface
{

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     */
    public function normalize(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        return $producerMessage;
    }
}
