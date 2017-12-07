<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use \InvalidArgumentException;
use RdKafka\Exception as RdKafkaException;

final class KafkaProducer extends AbstractKafkaProducer
{
    /**
     * @param string $message
     * @param string $topic
     * @throws KafkaProducerException
     * @return void
     */
    public function produce(string $message, string $topic)
    {
        $topicProducer = $this->getProducerTopicForTopic($topic);

        try {
            $topicProducer->produce($this->getPartition(), 0, $message);
        } catch (InvalidArgumentException | RdKafkaException $e) {
            throw new KafkaProducerException(
                $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }
}
