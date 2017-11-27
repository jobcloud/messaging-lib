<?php


namespace Jobcloud\Messaging\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Message;

final class KafkaProducerDeliveryReportCallback
{
    /**
     * @param RdKafkaProducer $producer
     * @param Message         $message
     * @throws KafkaProducerException
     * @return void
     */
    public function __invoke(RdKafkaProducer $producer, Message $message)
    {
        if ($message->err) {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
                    throw new KafkaProducerException(
                        sprint(KafkaProducerException::TIMEOUT_EXCEPTION_MESSAGE, $message->errstr()),
                        $message->err
                    );
                    break;
                default:
                    throw new KafkaProducerException(
                        sprintf(KafkaProducerException::UNEXPECTED_EXCEPTION_MESSAGE, $message->errstr()),
                        $message->err
                    );
                    break;
            }
        }
    }
}