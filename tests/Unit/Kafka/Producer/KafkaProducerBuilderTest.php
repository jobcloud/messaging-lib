<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;
use Jobcloud\Messaging\Producer\ProducerInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder
 */
class KafkaProducerBuilderTest extends TestCase
{

    /**
     * @var $kpb KafkaProducerBuilder
     */
    protected $kpb;

    public function setUp()
    {
        $this->kpb = KafkaProducerBuilder::create();
    }

    public function testGetConfig()
    {
        self::assertInternalType('array', $this->kpb->getConfig());
    }

    public function testGetBrokers()
    {
        self::assertInternalType('array', $this->kpb->getBrokers());
    }

    public function testSetConfig()
    {
        $this->kpb->setConfig(
            [
                'timeout' => 100
            ]
        );

        $config = $this->kpb->getConfig();

        self::assertEquals(['timeout' => 100], $config);
    }

    public function testAddBroker()
    {
        $this->kpb->addBroker('localhost');
        $brokers = $this->kpb->getBrokers();

        self::assertEquals(['localhost'], $brokers);
    }

    public function testSetDeliveryReportCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kpb->setDeliveryReportCallback($callback);

        self::assertAttributeEquals($callback, 'deliverReportCallback', $this->kpb);
    }

    public function testSetErrorCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kpb->setErrorCallback($callback);

        self::assertAttributeEquals($callback, 'errorCallback', $this->kpb);
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
}
