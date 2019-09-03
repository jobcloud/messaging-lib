<?php


namespace Jobcloud\Messaging\Kafka\Message\Normalizer;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;

interface NormalizerInterface
{
    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     */
    public function normalize(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface;
}
