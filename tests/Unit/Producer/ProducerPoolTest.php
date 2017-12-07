<?php

namespace Jobcloud\Messaging\Tests\Unit\Producer;

use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use Jobcloud\Messaging\Producer\ProducerPool;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

class ProducerPoolTest extends TestCase
{

    /**
     * @var $producerPool ProducerPool
     */
    protected $producerPool;

    public function setUp()
    {
        $this->producerPool = new ProducerPool();
    }

    public function getProducerMock()
    {
        $producerTopicMock = $this
            ->getMockBuilder(RdKafkaProducerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();
        $producerTopicMock
            ->expects(self::any())
            ->method('produce')
            ->with(RD_KAFKA_PARTITION_UA, 0, 'test');

        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->setMethods(['newTopic', 'addBrokers'])
            ->disableOriginalConstructor()
            ->getMock();

        $producerMock
            ->expects(self::any())
            ->method('newTopic')
            ->with('testTopic')
            ->willReturn(
                $producerTopicMock
            );

        $producerMock
            ->expects(self::any())
            ->method('addBrokers')
            ->with('localhost');

        return $producerMock;
    }

    public function testGetProducerPool()
    {
        self::assertInternalType('array', $this->producerPool->getProducerPool());
    }

    public function testAddProducerSuccess()
    {
        $producer = $producer = new KafkaProducer($this->getProducerMock(), ['localhost']);

        $this->producerPool->addProducer($producer);

        self::assertNotEmpty($this->producerPool->getProducerPool());
        self::assertTrue(1 == count($this->producerPool->getProducerPool()));
    }

    public function testAddProducerFail()
    {
        self::expectException('TypeError');
        $this->producerPool->addProducer('');
    }

    public function testProduce()
    {
        $producer = $producer = new KafkaProducer($this->getProducerMock(), ['localhost']);

        $this->producerPool->addProducer($producer);

        $result = $this->producerPool->produce('test', 'testTopic');

        self::assertNull($result);
    }
}
