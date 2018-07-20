<?php

namespace Jobcloud\Messaging\Tests\Unit\Producer;

use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use Jobcloud\Messaging\Producer\ProducerPool;
use PHPUnit\Framework\MockObject\MockObject;
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

    public function testGetProducerPool()
    {
        self::assertInternalType('array', $this->producerPool->getProducerPool());
    }

    public function testAddProducerSuccess()
    {
        $producerTopicMock = $this->getProducerTopicMock();
        $producerTopicMock
            ->expects(self::never())
            ->method('produce');

        $producer = $producer = new KafkaProducer($this->getProducerMock($producerTopicMock), ['localhost'], 0);

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
        $producerTopicMock = $this->getProducerTopicMock();
        $producerTopicMock
            ->expects(self::once())
            ->method('produce')
            ->with(RD_KAFKA_PARTITION_UA, 0, 'test');

        $rdKafkaProducer = $this->getProducerMock($producerTopicMock);
        $rdKafkaProducer
            ->expects(self::exactly(2))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );

        $rdKafkaProducer
            ->expects(self::once())
            ->method('poll')
            ->with(0);

        $producer = $producer = new KafkaProducer($rdKafkaProducer, ['localhost'], 0);

        $this->producerPool->addProducer($producer);
        $this->producerPool->produce('test', 'testTopic');
    }

    /**
     * @param RdKafkaProducerTopic $producerTopicMock
     * @return RdKafkaProducer|MockObject
     */
    private function getProducerMock(RdKafkaProducerTopic $producerTopicMock): RdKafkaProducer
    {
        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->setMethods(['newTopic', 'addBrokers', 'poll', 'getOutQLen'])
            ->disableOriginalConstructor()
            ->getMock();

        $producerMock
            ->expects(self::any())
            ->method('newTopic')
            ->with('testTopic')
            ->willReturn($producerTopicMock);

        $producerMock
            ->expects(self::any())
            ->method('addBrokers')
            ->with('localhost');

        return $producerMock;
    }

    /**
     * @return RdKafkaProducerTopic|MockObject
     */
    private function getProducerTopicMock(): RdKafkaProducerTopic
    {
        $producerTopicMock = $this
            ->getMockBuilder(RdKafkaProducerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();

        return $producerTopicMock;
    }
}
