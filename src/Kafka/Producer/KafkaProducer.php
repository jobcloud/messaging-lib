<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

final class KafkaProducer extends AbstractKafkaProducer
{

    /**
     * @param string  $message
     * @param string  $topic
     * @param integer $partition
     * @return void
     */
    public function produce(string $message, string $topic, int $partition = RD_KAFKA_PARTITION_UA)
    {
        $topicProducer = $this->getProducerTopicForTopic($topic);

        $topicProducer->produce($partition, 0, $message);

        $this->producer->poll($this->pollTimeout);
    }
}
