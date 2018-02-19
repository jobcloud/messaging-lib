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
        self::assertAttributeEquals(['localhost'], 'brokers', $this->kcb);
    }

    public function testSubscribeToTopic()
    {
        $topicSubscription = new TopicSubscription('testTopic');

        self::assertSame($this->kcb, $this->kcb->addSubscription($topicSubscription));
        self::assertAttributeEquals([$topicSubscription], 'topics', $this->kcb);
    }

    public function testSetTimeout()
    {
        $timeout = 42;

        self::assertSame($this->kcb, $this->kcb->setTimeout($timeout));
        self::assertAttributeEquals($timeout, 'timeout', $this->kcb);
    }

    public function testSetConfig()
    {
        $this->kcb->setConfig(
            [
              'timeout' => 100
            ]
        );

        self::assertAttributeEquals(['timeout' => 100], 'config', $this->kcb);
    }

    public function testSetConsumerGroup()
    {
        $this->kcb->setConsumerGroup('funGroup');

        self::assertAttributeEquals('funGroup', 'consumerGroup', $this->kcb);
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
