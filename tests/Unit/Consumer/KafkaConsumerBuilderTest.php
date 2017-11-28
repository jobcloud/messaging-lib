<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use PHPUnit\Framework\TestCase;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback;

/**
 * @covers Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder
 */
class KafkaConsumerBuilderTest extends TestCase
{

    protected $kcb;

    public function setUp()
    {
        $this->kcb = KafkaConsumerBuilder::create();
    }

    public function testCreate()
    {
        $this->assertInstanceOf(KafkaConsumerBuilder::class, KafkaConsumerBuilder::create());
    }

    public function testGetBrokers()
    {
        $this->assertInternalType('array', $this->kcb->getBrokers());
    }

    public function testGetConfig()
    {
        $this->assertInternalType('array', $this->kcb->getConfig());
    }


    public function testGetConsumerGroup()
    {
        $consumerGroup = $this->kcb->getConsumerGroup();
        $this->assertInternalType('string', $consumerGroup);
        $this->assertEquals('default', $consumerGroup);
    }

    public function testGetErrorCallback()
    {
        $this->assertInstanceOf(KafkaConsumerErrorCallback::class, $this->kcb->getErrorCallback());
    }

    public function testGetRebalanceCallback()
    {
        $this->assertInstanceOf(KafkaConsumerRebalanceCallback::class, $this->kcb->getRebalanceCallback());
    }

    public function testGetTopics()
    {
        $this->assertInternalType('array', $this->kcb->getTopics());
    }

    public function testAddBroker()
    {
        $this->kcb->addBroker('localhost');
        $brokers = $this->kcb->getBrokers();

        $this->assertEquals(['localhost'], $brokers);
    }

    public function testSubscribeToTopic()
    {
        $this->kcb->subscribeToTopic('testTopic');
        $topics = $this->kcb->getTopics();

        $this->assertEquals(['testTopic'], $topics);
    }

    public function testSetConfig()
    {
        $this->kcb->setConfig(
            [
              'timeout' => 100
            ]
        );

        $config = $this->kcb->getConfig();

        $this->assertEquals(['timeout' => 100], $config);
    }

    public function testSetConsumerGroup()
    {
        $this->kcb->setConsumerGroup('funGroup');
        $consumerGroup = $this->kcb->getConsumerGroup();

        $this->assertEquals('funGroup', $consumerGroup);
    }

    public function testSetErrorCallback()
    {
        $callback = function() { echo 'foo'; };

        $this->kcb->setErrorCallback($callback);

        $this->assertEquals($callback, $this->kcb->getErrorCallback());
    }

    public function testSetRebalanceCallback()
    {
        $callback = function() { echo 'foo'; };

        $this->kcb->setRebalanceCallback($callback);

        $this->assertEquals($callback, $this->kcb->getRebalanceCallback());
    }

    /**
     * @expectedException Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException
     * @expectedException Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException
     */
    public function testBuildFail()
    {
        $this->kcb
            ->addBroker('localhost')
            ->build();


    }

    public function testBuildSuccess()
    {
        $callback = function ($kafka, $errId, $msg) {};

        /**
         * @var $consumer KafkaConsumer
         */
        $consumer = KafkaConsumerBuilder::create()
            ->addBroker('localhost')
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->build();

        $this->assertInstanceOf(ConsumerInterface::class, $consumer);
    }
}