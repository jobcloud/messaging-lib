<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

final class KafkaConsumer extends AbstractKafkaConsumer
{
    /**
     * @param int $timeout
     * @return Message
     */
    public function consume(int $timeout)
    {
        $message = $this->consumer->consume($timeout);
        $msg = new Message($message->payload);

        return $msg;
    }
}
