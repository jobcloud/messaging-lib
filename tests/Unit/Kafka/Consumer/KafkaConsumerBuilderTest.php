<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumer;
use Jobcloud\Messaging\Kafka\Consumer\KafkaLowLevelConsumer;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilderException;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use PHPUnit\Framework\TestCase;
use RdKafka\Consumer;
use RdKafka\KafkaConsumer;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder
 */
final class KafkaConsumerBuilderTest extends TestCase
{

    /** @var string */
    private const TEST_BROKER = 'TEST_BROKER';
    /** @var string */
    private const TEST_TOPIC = 'TEST_TOPIC';
    /** @var int */
    private const TEST_TIMEOUT = 9999;
    /** @var string */
    private const TEST_CONSUMER_GROUP = 'TEST_CONSUMER_GROUP';

    /** @var KafkaConsumerBuilder */
    private $kafkaConsumerBuilder;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->kafkaConsumerBuilder = KafkaConsumerBuilder::create();
    }

    /**
     * @return void
     */
    public function testCreate(): void
    {
        self::assertInstanceOf(KafkaConsumerBuilder::class, KafkaConsumerBuilder::create());
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testAddBroker(): void
    {
        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->addBroker(self::TEST_BROKER));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame([self::TEST_BROKER], $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSubscribeToLowLevelTopic(): void
    {
        $topicSubscription = new TopicSubscription(self::TEST_TOPIC);

        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->addLowLevelSubscription($topicSubscription));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'topics');
        $reflectionProperty->setAccessible(true);

        self::assertSame([$topicSubscription], $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSubscribeToHighKLevelTopic(): void
    {
        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->addSubscription(self::TEST_TOPIC));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'topics');
        $reflectionProperty->setAccessible(true);

        self::assertSame([self::TEST_TOPIC], $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetTimeout(): void
    {
        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->setTimeout(self::TEST_TIMEOUT));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'timeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame(self::TEST_TIMEOUT, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetConfig(): void
    {
        $config = ['timeout' => self::TEST_TIMEOUT];
        $this->kafkaConsumerBuilder->setConfig($config);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame($config, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetConsumerGroup(): void
    {
        $this->kafkaConsumerBuilder->setConsumerGroup(self::TEST_CONSUMER_GROUP);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'consumerGroup');
        $reflectionProperty->setAccessible(true);

        self::assertSame(self::TEST_CONSUMER_GROUP, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetConsumerTypeLow(): void
    {
        $this->kafkaConsumerBuilder->setConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL);

        $actualConsumerType = new \ReflectionProperty($this->kafkaConsumerBuilder, 'consumerType');
        $actualConsumerType->setAccessible(true);
        $actualRdKafkaConsumerType = new \ReflectionProperty($this->kafkaConsumerBuilder, 'rdKafkaConsumerType');
        $actualRdKafkaConsumerType->setAccessible(true);

        self::assertSame(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL, $actualConsumerType->getValue($this->kafkaConsumerBuilder));
        self::assertSame(Consumer::class, $actualRdKafkaConsumerType->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetConsumerTypeHigh(): void
    {
        $this->kafkaConsumerBuilder->setConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_HIGH_LEVEL);

        $actualConsumerType = new \ReflectionProperty($this->kafkaConsumerBuilder, 'consumerType');
        $actualConsumerType->setAccessible(true);
        $actualRdKafkaConsumerType = new \ReflectionProperty($this->kafkaConsumerBuilder, 'rdKafkaConsumerType');
        $actualRdKafkaConsumerType->setAccessible(true);

        self::assertSame(KafkaConsumerBuilder::CONSUMER_TYPE_HIGH_LEVEL, $actualConsumerType->getValue($this->kafkaConsumerBuilder));
        self::assertSame(KafkaConsumer::class, $actualRdKafkaConsumerType->getValue($this->kafkaConsumerBuilder));
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

        $this->kafkaConsumerBuilder->setErrorCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'errorCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetRebalanceCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $this->kafkaConsumerBuilder->setRebalanceCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'rebalanceCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws KafkaConsumerBuilderException
     */
    public function testBuildFailMissingBrokers(): void
    {
        self::expectException(KafkaConsumerBuilderException::class);

        $this->kafkaConsumerBuilder->build();
    }

    /**
     * @return void
     * @throws KafkaConsumerBuilderException
     */
    public function testBuildFailMissingTopics(): void
    {
        self::expectException(KafkaConsumerBuilderException::class);

        $this->kafkaConsumerBuilder->addBroker(self::TEST_BROKER)->build();
    }

    /**
     * @return void
     */
    public function testBuildSuccess(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        /** @var $consumer KafkaLowLevelConsumer */
        $consumer = $this->kafkaConsumerBuilder
            ->addBroker(self::TEST_BROKER)
            ->addLowLevelSubscription(new TopicSubscription(self::TEST_TOPIC))
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->build();

        self::assertInstanceOf(KafkaConsumerInterface::class, $consumer);
        self::assertInstanceOf(KafkaHighLevelConsumer::class, $consumer);
    }

    /**
     * @return void
     */
    public function testBuildLowLevelSuccess(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        /** @var $consumer KafkaLowLevelConsumer */
        $consumer = $this->kafkaConsumerBuilder
            ->addBroker(self::TEST_BROKER)
            ->addLowLevelSubscription(new TopicSubscription(self::TEST_TOPIC))
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->setConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL)
            ->build();

        self::assertInstanceOf(KafkaConsumerInterface::class, $consumer);
        self::assertInstanceOf(KafkaLowLevelConsumer::class, $consumer);
    }
}
