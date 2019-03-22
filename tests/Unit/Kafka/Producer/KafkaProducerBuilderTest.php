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

    public function setUp()
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

        self::assertAttributeEquals(['timeout' => 100], 'config', $this->kafkaProducerBuilder);
    }

    public function testAddBroker()
    {
        $this->kafkaProducerBuilder->addBroker('localhost');

        self::assertAttributeEquals(['localhost'], 'brokers', $this->kafkaProducerBuilder);
    }

    public function testSetDeliveryReportCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kafkaProducerBuilder->setDeliveryReportCallback($callback);

        self::assertAttributeEquals($callback, 'deliverReportCallback', $this->kafkaProducerBuilder);
    }

    public function testSetErrorCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kafkaProducerBuilder->setErrorCallback($callback);

        self::assertAttributeEquals($callback, 'errorCallback', $this->kafkaProducerBuilder);
    }

    public function testSetPollTimeout()
    {
        $pollTimeout = 42;

        $this->kafkaProducerBuilder->setPollTimeout($pollTimeout);

        self::assertAttributeEquals($pollTimeout, 'pollTimeout', $this->kafkaProducerBuilder);
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

        self::assertAttributeEquals(
            [
                'socket.timeout.ms' => 50,
                'internal.termination.signal' => SIGIO
            ],
            'config',
            $producerBuilder
        );
    }
}
