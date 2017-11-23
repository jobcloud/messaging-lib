<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

final class Consumer extends AbstractConsumer
{
    public function consume(int $timeout)
    {
        $message = $this->consumer->consume($timeout);
        $msg = new Message($message->payload);

        return $msg;
    }
}
