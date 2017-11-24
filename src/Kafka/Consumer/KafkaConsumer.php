<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\Message;

final class KafkaConsumer extends AbstractKafkaConsumer
{
    /**
     * @param int $timeout
     * @return Message
     */
    public function consume(int $timeout): Message
    {
        $message = $this->consumer->consume($timeout);

        return $message;
    }
}
