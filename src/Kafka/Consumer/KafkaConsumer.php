<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use RdKafka\Message;

final class KafkaConsumer extends AbstractKafkaConsumer
{
    /**
     * @param integer $timeout
     * @return Message
     * @throws KafkaConsumerException
     */
    public function consume(int $timeout): Message
    {
        $message = $this->getConsumer()->consume($timeout);

        if (null !== $message->err
            && RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err
            && RD_KAFKA_RESP_ERR__PARTITION_EOF !== $message->err
        ) {
            throw new KafkaConsumerException(
                sprintf(KafkaConsumerException::CONSUMPTION_EXCEPTION_MESSAGE, $message->errstr()),
                $message->err
            );
        }

        return $message;
    }
}
