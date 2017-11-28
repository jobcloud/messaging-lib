<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use RdKafka\Message;

/**
 * @covers Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer
 * @covers Jobcloud\Messaging\Kafka\Consumer\AbstractKafkaConsumer
 */
class KafkaConsumerTest extends TestCase
{
    public function testConsume()
    {
        $callback = function ($kafka, $errId, $msg) {};

        /**
         * @var $consumer KafkaConsumer
         */
        $consumer = KafkaConsumerBuilder::create()
            ->addBroker('localhost')
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->build();

        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->setMethods(['consume'])
            ->disableOriginalConstructor()
            ->getMock();

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willReturn(
                new Message()
            );

       $consumer->setConsumer($consumerMock);

        $this->assertInstanceOf(Message::class, $consumer->consume(1));
    }
}