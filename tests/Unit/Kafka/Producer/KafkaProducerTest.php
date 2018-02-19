<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use PHPUnit\Framework\MockObject\MockObject;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use RdKafka\Producer as RdKafkaProducer;
use PHPUnit\Framework\TestCase;
use RdKafka\ProducerTopic;
use RdKafka\Conf;
use \InvalidArgumentException;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducer
 * @covers \Jobcloud\Messaging\Kafka\Producer\AbstractKafkaProducer
 */
class KafkaProducerTest extends TestCase
{

    public function testGetProducerTopicForTopic()
    {
        $producerTopicMock = $this->getMockBuilder(ProducerTopic::class)
            ->disableOriginalConstructor()
            ->getMock();

        $rdKafkaProducer = $this->getRdKafkaProducer();
        $rdKafkaProducer
            ->expects(self::once())
            ->method('newTopic')
            ->with('testTopic')
            ->willReturn($producerTopicMock);

        $producer = new KafkaProducer($rdKafkaProducer, ['localhost'], 0);

        $producerTopic = $producer->getProducerTopicForTopic('testTopic');

        self::assertSame($producerTopicMock, $producerTopic);
    }

    public function testProduceError()
    {
        self::expectException(KafkaProducerException::class);

        $producerTopicMock = $this->getMockBuilder(RdKafkaProducerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();

        $producerTopicMock
            ->expects(self::once())
            ->method('produce')
            ->with(RD_KAFKA_PARTITION_UA, 0, 'test')
            ->willThrowException(new KafkaProducerException());

        /** @var MockObject|RdKafkaProducer $producerMock */
        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->setMethods(['newTopic', 'addBrokers'])
            ->disableOriginalConstructor()
            ->getMock();

        $producerMock
            ->expects(self::any())
            ->method('addBrokers')
            ->with('localhost');

        $producerMock
            ->expects(self::any())
            ->method('newTopic')
            ->willReturn($producerTopicMock);


        $producer = new KafkaProducer($producerMock, ['localhost'], 0);

        $producer->produce('test', 'test');
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

        $producerMock = $this->getRdKafkaProducer();

        $producerMock
            ->expects(self::any())
            ->method('newTopic')
            ->willReturn($producerTopicMock);

        $producerMock
            ->expects(self::once())
            ->method('poll')
            ->with(0);

        $producerMock
            ->expects(self::any())
            ->method('addBrokers')
            ->with('localhost');

        $producer = new KafkaProducer($producerMock, ['localhost'], 0);

        $producer->produce('test', 'test');
    }

    /**
     * @return MockObject|RdKafkaProducer $producerMock
     */
    private function getRdKafkaProducer(): RdKafkaProducer
    {
        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->setMethods(['newTopic', 'addBrokers', 'poll'])
            ->disableOriginalConstructor()
            ->getMock();

        return $producerMock;
    }
}
