<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use RdKafka\Producer as RdKafkaProducer;
use PHPUnit\Framework\TestCase;
use RdKafka\ProducerTopic;
use RdKafka\Conf;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducer
 * @covers \Jobcloud\Messaging\Kafka\Producer\AbstractKafkaProducer
 *
 */
class KafkaProducerTest extends TestCase
{

    /**
     * @var $producer KafkaProducer
     */
    protected $producer;

    /**
     * @var $rdProducer RdKafkaProducer
     */
    protected $rdProducer;

    public function setUp()
    {
        $callback = function ($kafka, $errId, $msg) {
            //do nothing
        };

        $conf = new Conf();
        $conf->setErrorCb($callback);
        $conf->setDrMsgCb($callback);
        $this->rdProducer = new RdKafkaProducer($conf);

        $this->producer = new KafkaProducer($this->rdProducer, ['localhost']);
    }


    public function testGetProducerTopicForTopic()
    {
        $producerTopic = $this->producer->getProducerTopicForTopic('testTopic');

        self::assertInstanceOf(ProducerTopic::class, $producerTopic);
    }

    public function testGetPartition()
    {
        self::assertEquals(RD_KAFKA_PARTITION_UA, $this->producer->getPartition());
    }

    public function testProduceError()
    {
        self::expectException('Jobcloud\Messaging\Kafka\Exception\KafkaProducerException');

        $producerMock = $this->getMockBuilder(KafkaProducer::class)
            ->setConstructorArgs(
                [
                    $this->rdProducer,
                    ['localhost']
                ]
            )
            ->setMethods(['getPartition'])
            ->getMock();

        $producerMock
            ->expects(self::any())
            ->method('getPartition')
            ->willReturn(-100);

        $producerMock->produce('test', 'test');
    }

    public function testProduceSuccess()
    {
        $producerTopicMock = $this
            ->getMockBuilder(RdKafkaProducerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();
        $producerTopicMock
            ->expects(self::once())
            ->method('produce')
            ->with(RD_KAFKA_PARTITION_UA, 0, 'test');

        $producerMock = $this->getMockBuilder(KafkaProducer::class)
            ->setConstructorArgs(
                [
                    $this->rdProducer,
                    ['localhost']
                ]
            )
            ->setMethods(['getProducerTopicForTopic'])
            ->getMock();

        $producerMock
            ->expects(self::once())
            ->method('getProducerTopicForTopic')
            ->with('test')
            ->willReturn(
                $producerTopicMock
            );

        $producerMock->produce('test', 'test');
    }
}