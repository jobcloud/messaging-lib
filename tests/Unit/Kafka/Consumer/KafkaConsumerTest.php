<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use PHPUnit\Framework\TestCase;
use RdKafka\Conf;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer
 * @covers \Jobcloud\Messaging\Kafka\Consumer\AbstractKafkaConsumer
 */
class KafkaConsumerTest extends TestCase
{

    protected $consumer;

    public function setUp()
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

        $this->consumer = new KafkaConsumer($rdKafkaConsumer, ['test']);
    }


    public function testConsumeSuccess()
    {
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

        $ref = new \ReflectionProperty(KafkaConsumer::class, 'consumer');
        $ref->setAccessible(true);
        $ref->setValue($this->consumer, $consumerMock);


        self::assertInstanceOf(Message::class, $this->consumer->consume(1));
    }

    public function testConsumeFail()
    {
        self::expectException('Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException');

        $message = new Message();
        $message->err = -1;

        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->setMethods(['consume'])
            ->disableOriginalConstructor()
            ->getMock();

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willReturn(
                $message
            );

        $ref = new \ReflectionProperty(KafkaConsumer::class, 'consumer');
        $ref->setAccessible(true);
        $ref->setValue($this->consumer, $consumerMock);


        self::assertInstanceOf(Message::class, $this->consumer->consume(1));
    }
}