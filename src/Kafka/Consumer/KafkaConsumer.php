<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use RdKafka\Exception as RdKafkaException;

final class KafkaConsumer extends AbstractKafkaConsumer
{

    /**
     * @param integer $timeout
     * @return MessageInterface|Message
     * @throws KafkaConsumerException
     */
    public function consume(int $timeout): MessageInterface
    {
        try {
            $message = $this->consumer->consume($timeout);

            if (null !== $message->err
                && RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err
                && RD_KAFKA_RESP_ERR__PARTITION_EOF !== $message->err
            ) {
                throw new KafkaConsumerException(
                    sprintf(KafkaConsumerException::CONSUMPTION_EXCEPTION_MESSAGE, $message->errstr()),
                    $message->err
                );
            }

            return new Message(
                $message->payload,
                $message->topic_name,
                $message->partition,
                $message->offset,
                $message->err,
                $message->errstr()
            );
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerException($e->getMessage(), $e->getCode(), $e);
        }
    }
}
