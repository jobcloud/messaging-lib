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

    /**
     * @var $kafkaProducerBuilder KafkaProducerBuilder
     */
    protected $kafkaProducerBuilder;

    public function setUp(): void
    {
        $this->kafkaProducerBuilder = KafkaProducerBuilder::create();
    }

    public function testSetConfig()
    {
        $this->kafkaProducerBuilder->setConfig(
            [
                'timeout' => 100
            ]
        );

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['timeout' => 100], $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    public function testAddBroker()
    {
        $this->kafkaProducerBuilder->addBroker('localhost');

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['localhost'], $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    public function testSetDeliveryReportCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kafkaProducerBuilder->setDeliveryReportCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'deliverReportCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    public function testSetErrorCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kafkaProducerBuilder->setErrorCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'errorCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    public function testSetPollTimeout()
    {
        $pollTimeout = 42;

        $this->kafkaProducerBuilder->setPollTimeout($pollTimeout);

        $reflectionProperty = new \ReflectionProperty($this->kafkaProducerBuilder, 'pollTimeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame($pollTimeout, $reflectionProperty->getValue($this->kafkaProducerBuilder));
    }

    public function testBuildNoBroker()
    {
        self::expectException(KafkaProducerException::class);

        KafkaProducerBuilder::create()->build();
    }

    public function testBuild()
    {
        $callback = function ($kafka, $errId, $msg) {
            //do nothing
        };

        $producer = KafkaProducerBuilder::create()
            ->addBroker('localhost')
            ->setDeliveryReportCallback($callback)
            ->build();

        self::assertInstanceOf(ProducerInterface::class, $producer);
    }

    public function testKafkaProducerBuilderConfig()
    {
        $callback = function ($kafka, $errId, $msg) {
            //do nothing
        };

        $producerBuilder = KafkaProducerBuilder::create();

        $producerBuilder->addBroker('localhost')->setDeliveryReportCallback($callback)->build();

        $reflectionProperty = new \ReflectionProperty($producerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame([
            'socket.timeout.ms' => 50,
            'internal.termination.signal' => SIGIO
        ], $reflectionProperty->getValue($producerBuilder));
    }
}
