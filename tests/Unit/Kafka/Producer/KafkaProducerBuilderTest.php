<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;
use Jobcloud\Messaging\Producer\ProducerInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder
 */
class KafkaProducerBuilderTest extends TestCase
{
    /** @var int */
    private const TEST_TIMEOUT = 9999;
    /** @var string */
    private const TEST_BROKER = 'TEST_BROKER';

    /** @var $kafkaProducerBuilder KafkaProducerBuilder */
    protected $kafkaProducerBuilder;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->kafkaProducerBuilder = KafkaProducerBuilder::create();
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetConfig(): void
    {
        $config = ['timeout' => self::TEST_TIMEOUT];
        $this->kafkaProducerBuilder->setConfig($config);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame($config, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testAddBroker(): void
    {
        $this->kafkaProducerBuilder->addBroker(self::TEST_BROKER);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame([self::TEST_BROKER], $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetDeliveryReportCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $this->kafkaProducerBuilder->setDeliveryReportCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'deliverReportCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetErrorCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $this->kafkaProducerBuilder->setErrorCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'errorCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetPollTimeout(): void
    {
        $this->kafkaProducerBuilder->setPollTimeout(self::TEST_TIMEOUT);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'pollTimeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame(self::TEST_TIMEOUT, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    /**
     * @return void
     */
    public function testBuildNoBroker(): void
    {
        self::expectException(KafkaProducerException::class);

        $this->kafkaProducerBuilder->build();
    }

    /**
     * @return void
     */
    public function testBuild(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        $producer = $this->kafkaProducerBuilder
            ->addBroker(self::TEST_BROKER)
            ->setDeliveryReportCallback($callback)
            ->build();

        self::assertInstanceOf(ProducerInterface::class, $producer);
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testKafkaProducerBuilderConfig(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        $this->kafkaProducerBuilder->addBroker('localhost')->setDeliveryReportCallback($callback)->build();

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame(
            [
                'socket.timeout.ms' => 50,
                'internal.termination.signal' => SIGIO
            ],
            $reflectionProperty->getValue($this->kafkaProducerBuilder)
        );
    }
}
