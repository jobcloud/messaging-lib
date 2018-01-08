<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Message;

final class KafkaProducerDeliveryReportCallback
{
    /**
     * @param RdKafkaProducer $producer
     * @param Message         $message
     * @return void
     * @throws KafkaProducerException
     */
    public function __invoke(RdKafkaProducer $producer, Message $message)
    {
        if (RD_KAFKA_RESP_ERR_NO_ERROR === $message->err) {
            return;
        }

        throw new KafkaProducerException(
            $message->errstr(),
            $message->err
        );
    }
}
