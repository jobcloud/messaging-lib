<?php


namespace Jobcloud\Kafka\Consumer;


use Jobcloud\Kafka\Message\Message;
use Jobcloud\Kafka\Message\MessageInterface;

final class Consumer extends AbstractConsumer
{
    public function consume(int $timeout): MessageInterface
    {
        $message = $this->consumer->consume($timeout);
        $msg = new Message($message->payload);

        return $msg;
    }
}