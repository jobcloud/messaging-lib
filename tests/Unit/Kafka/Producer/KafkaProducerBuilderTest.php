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
        $config = ['timeout' => 1000];
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
        $this->kafkaProducerBuilder->addBroker('localhost');

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['localhost'], $reflectionProperty->getValue($this->kafkaProducerBuilder));
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
        $this->kafkaProducerBuilder->setPollTimeout(1000);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'pollTimeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame(1000, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    /**
     * @throws KafkaProducerException
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
            ->addBroker('localhost')
            ->setDeliveryReportCallback($callback)
            ->setErrorCallback($callback)
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

        $this->kafkaProducerBuilder
            ->addBroker('localhost')
            ->setDeliveryReportCallback($callback)
            ->setErrorCallback($callback)
            ->build();

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
