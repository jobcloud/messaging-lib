<?php

namespace Jobcloud\Messaging\Tests\Unit\Producer;

use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use Jobcloud\Messaging\Producer\ProducerInterface;
use Jobcloud\Messaging\Producer\ProducerPool;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class ProducerPoolTest extends TestCase
{

    /** @var string */
    private const TEST_MESSAGE = 'TEST_MESSAGE';
    /** @var string */
    private const TEST_TOPIC = 'TEST_TOPIC';
    /** @var int */
    private const TEST_PARTITION = 999999;
    /** @var string */
    private const TEST_KEY = 'TEST_KEY';

    /** @var ProducerInterface|MockObject */
    private $kafkaProducerMock;

    /** @var $producerPool ProducerPool */
    private $producerPool;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->kafkaProducerMock = $this->createMock(ProducerInterface::class);
        $this->producerPool = new ProducerPool();
    }

    /**
     * @
     */
    public function testAddProducer(): void
    {
        $this->producerPool->addProducer($this->kafkaProducerMock);
        $producers = $this->producerPool->getProducerPool();

        self::assertIsArray($producers);
        self::assertNotEmpty($producers);
        self::assertTrue(1 == count($this->producerPool->getProducerPool()));
    }

    /**
     * @return void
     */
    public function testProduce(): void
    {
        $this->kafkaProducerMock
            ->expects(self::once())
            ->method('produce')
            ->with(self::TEST_MESSAGE, self::TEST_TOPIC, self::TEST_PARTITION, self::TEST_KEY);
        $this->producerPool->addProducer($this->kafkaProducerMock);
        $this->producerPool->produce(self::TEST_MESSAGE, self::TEST_TOPIC, self::TEST_PARTITION, self::TEST_KEY);
    }
}
