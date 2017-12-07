<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use PHPUnit\Framework\TestCase;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder
 */
class KafkaConsumerBuilderTest extends TestCase
{

    /**
     * @var $kcb KafkaConsumerBuilder
     */
    protected $kcb;

    public function setUp()
    {
        $this->kcb = KafkaConsumerBuilder::create();
    }

    public function testCreate()
    {
        self::assertInstanceOf(KafkaConsumerBuilder::class, KafkaConsumerBuilder::create());
    }

    public function testGetBrokers()
    {
        self::assertInternalType('array', $this->kcb->getBrokers());
    }

    public function testGetConfig()
    {
        self::assertInternalType('array', $this->kcb->getConfig());
    }

    public function testGetConsumerGroup()
    {
        $consumerGroup = $this->kcb->getConsumerGroup();
        self::assertInternalType('string', $consumerGroup);
        self::assertEquals('default', $consumerGroup);
    }

    public function testGetTopics()
    {
        self::assertInternalType('array', $this->kcb->getTopics());
    }

    public function testAddBroker()
    {
        $this->kcb->addBroker('localhost');
        $brokers = $this->kcb->getBrokers();

        self::assertEquals(['localhost'], $brokers);
    }

    public function testSubscribeToTopic()
    {
        $this->kcb->subscribeToTopic('testTopic');
        $topics = $this->kcb->getTopics();

        self::assertEquals(['testTopic'], $topics);
    }

    public function testSetConfig()
    {
        $this->kcb->setConfig(
            [
              'timeout' => 100
            ]
        );

        $config = $this->kcb->getConfig();

        self::assertEquals(['timeout' => 100], $config);
    }

    public function testSetConsumerGroup()
    {
        $this->kcb->setConsumerGroup('funGroup');
        $consumerGroup = $this->kcb->getConsumerGroup();

        self::assertEquals('funGroup', $consumerGroup);
    }

    public function testSetErrorCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kcb->setErrorCallback($callback);

        self::assertAttributeEquals($callback, 'errorCallback', $this->kcb);
    }

    public function testSetRebalanceCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kcb->setRebalanceCallback($callback);

        self::assertAttributeEquals($callback, 'rebalanceCallback', $this->kcb);
    }

    public function testBuildFail()
    {
        self::expectException(KafkaConsumerException::class);

        $this->kcb
            ->build();
    }

    public function testBuildFailConsumer()
    {
        self::expectException(KafkaConsumerException::class);

        $this->kcb
            ->addBroker('localhost')
            ->setConsumerGroup('')
            ->build();
    }

    public function testBuildSuccess()
    {
        $callback = function ($kafka, $errId, $msg) {
            //do nothing
        };

        /**
         * @var $consumer KafkaConsumer
         */
        $consumer = KafkaConsumerBuilder::create()
            ->addBroker('localhost')
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->build();

        self::assertInstanceOf(ConsumerInterface::class, $consumer);
    }
}
