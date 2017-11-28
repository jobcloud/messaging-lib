<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use PHPUnit\Framework\TestCase;
use RdKafka\Conf;
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
        $callback = function ($kafka, $errId, $msg) {
            //do nothing
        };

        $conf = new Conf();
        $conf->setErrorCb($callback);
        $conf->setRebalanceCb($callback);
        $conf->set('metadata.broker.list', 'localhost');
        $conf->set('group.id', 'defaultGroup');
        $rdKafkaConsumer = new RdKafkaConsumer($conf);

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

        $consumer = new KafkaConsumer($rdKafkaConsumer, ['test']);

        $ref = new \ReflectionProperty(KafkaConsumer::class, 'consumer');
        $ref->setAccessible(true);
        $ref->setValue($consumer, $consumerMock);


        self::assertInstanceOf(Message::class, $consumer->consume(1));
    }
}