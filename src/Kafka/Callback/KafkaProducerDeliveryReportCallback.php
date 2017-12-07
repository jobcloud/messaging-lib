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
        if (null === $message->err) {
            return;
        }

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
                throw new KafkaProducerException(
                    sprintf(KafkaProducerException::TIMEOUT_EXCEPTION_MESSAGE, $message->errstr()),
                    $message->err
                );
            default:
                throw new KafkaProducerException(
                    sprintf(KafkaProducerException::UNEXPECTED_EXCEPTION_MESSAGE, $message->errstr()),
                    $message->err
                );
        }
    }
}
