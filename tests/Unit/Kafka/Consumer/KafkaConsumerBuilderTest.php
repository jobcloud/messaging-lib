<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilderException;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use PHPUnit\Framework\TestCase;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder
 */
final class KafkaConsumerBuilderTest extends TestCase
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

    public function testAddBroker()
    {
        self::assertSame($this->kcb, $this->kcb->addBroker('localhost'));

        $property = new \ReflectionProperty($this->kcb, 'brokers');
        $property->setAccessible(true);

        $brokers = $property->getValue($this->kcb);

        self::assertEquals(['localhost'], $brokers);
    }

    public function testSubscribeToTopic()
    {
        $topicSubscription = new TopicSubscription('testTopic');

        self::assertSame($this->kcb, $this->kcb->addSubscription($topicSubscription));

        $property = new \ReflectionProperty($this->kcb, 'topics');
        $property->setAccessible(true);

        $topics = $property->getValue($this->kcb);

        self::assertEquals([$topicSubscription], $topics);
    }

    public function testSetTimeout()
    {
        $timeout = 42;

        self::assertSame($this->kcb, $this->kcb->setTimeout($timeout));

        $property = new \ReflectionProperty($this->kcb, 'timeout');
        $property->setAccessible(true);

        $storedTimeout = $property->getValue($this->kcb);

        self::assertEquals($timeout, $storedTimeout);
    }

    public function testSetConfig()
    {
        $this->kcb->setConfig(
            [
              'timeout' => 100
            ]
        );

        $property = new \ReflectionProperty($this->kcb, 'config');
        $property->setAccessible(true);

        $config = $property->getValue($this->kcb);

        self::assertEquals(['timeout' => 100], $config);
    }

    public function testSetConsumerGroup()
    {
        $this->kcb->setConsumerGroup('funGroup');

        $property = new \ReflectionProperty($this->kcb, 'consumerGroup');
        $property->setAccessible(true);

        $consumerGroup = $property->getValue($this->kcb);

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
        self::expectException(KafkaConsumerBuilderException::class);

        $this->kcb->build();
    }

    public function testBuildFailConsumer()
    {
        self::expectException(KafkaConsumerBuilderException::class);

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
            ->addSubscription(new TopicSubscription('test'))
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->build();

        self::assertInstanceOf(ConsumerInterface::class, $consumer);
    }
}
