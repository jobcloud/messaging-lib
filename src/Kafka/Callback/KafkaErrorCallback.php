<?php

namespace Jobcloud\Messaging\Kafka\Callback;

use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException;

final class KafkaErrorCallback
{

    /**
     * @param RdKafkaConsumer $consumer
     * @param integer         $errorCode
     * @param string          $reason
     * @throws KafkaBrokerException
     * @return void
     */
    public function __invoke(RdKafkaConsumer $consumer, int $errorCode, string $reason)
    {
        throw new KafkaBrokerException(
            sprintf(KafkaBrokerException::BROKER_EXCEPTION_MESSAGE, $reason),
            $errorCode
        );
    }
}