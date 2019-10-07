<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilderException;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use PHPUnit\Framework\TestCase;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder
 */
final class KafkaConsumerBuilderTest extends TestCase
{

    /**
     * @var $kcb KafkaConsumerBuilder
     */
    protected $kcb;

    public function setUp(): void
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

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            ['localhost'],
            $reflectionProperty->getValue($this->kcb)
        );
    }

    public function testSubscribeToTopic()
    {
        $topicSubscription = new TopicSubscription('testTopic');

        self::assertSame($this->kcb, $this->kcb->addSubscription($topicSubscription));

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'topics');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            [$topicSubscription],
            $reflectionProperty->getValue($this->kcb)
        );
    }

    public function testSetTimeout()
    {
        $timeout = 42;

        self::assertSame($this->kcb, $this->kcb->setTimeout($timeout));

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'timeout');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            $timeout,
            $reflectionProperty->getValue($this->kcb)
        );
    }

    public function testSetConfig()
    {
        $this->kcb->setConfig(
            [
              'timeout' => 100
            ]
        );

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            ['timeout' => 100],
            $reflectionProperty->getValue($this->kcb)
        );
    }

    public function testSetConsumerGroup()
    {
        $this->kcb->setConsumerGroup('funGroup');

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'consumerGroup');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            'funGroup',
            $reflectionProperty->getValue($this->kcb)
        );
    }

    public function testSetErrorCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kcb->setErrorCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'errorCallback');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            $callback,
            $reflectionProperty->getValue($this->kcb)
        );
    }

    public function testSetRebalanceCallback()
    {
        $callback = function () {
            echo 'foo';
        };

        $this->kcb->setRebalanceCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kcb, 'rebalanceCallback');
        $reflectionProperty->setAccessible(true);

        self::assertEquals(
            $callback,
            $reflectionProperty->getValue($this->kcb)
        );
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

        self::assertInstanceOf(KafkaConsumerInterface::class, $consumer);
    }
}
