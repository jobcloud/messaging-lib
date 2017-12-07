<?php

namespace Jobcloud\Messaging\Tests\Unit\Producer;

use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use Jobcloud\Messaging\Producer\ProducerPool;
use PHPUnit\Framework\TestCase;

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
        $producer = $this->
        getMockBuilder(KafkaProducer::class)
        ->disableOriginalConstructor()
        ->getMock();

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
        $producer = $this->
        getMockBuilder(KafkaProducer::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();

        $producer
            ->expects(self::once())
            ->method('produce')
            ->with('test', 'testTopic');

        $this->producerPool->addProducer($producer);

        $this->producerPool->produce('test', 'testTopic');
    }
}
