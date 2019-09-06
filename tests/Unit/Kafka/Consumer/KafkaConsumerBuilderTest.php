<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumer;
use Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\KafkaLowLevelConsumer;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerBuilderException;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerInterface;
use Jobcloud\Messaging\Kafka\Consumer\KafkaLowLevelConsumerInterface;
use PHPUnit\Framework\TestCase;
use RdKafka\Consumer;
use RdKafka\KafkaConsumer;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder
 */
final class KafkaConsumerBuilderTest extends TestCase
{

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
        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->addBroker('localhost'));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['localhost'], $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSubscribeToTopic(): void
    {
        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->addSubscription('test-topic'));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'topics');
        $reflectionProperty->setAccessible(true);

        self::isInstanceOf(TopicSubscription::class, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetTimeout(): void
    {
        self::assertSame($this->kafkaConsumerBuilder, $this->kafkaConsumerBuilder->setTimeout(1000));

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'timeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame(1000, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testAddConfig(): void
    {
        $intialConfig = ['timeout' => 1000, 'group.id' => 'test-group'];
        $newConfig = ['timeout' => 1001, 'offset.store.sync.interval.ms' => 60e3];
        $this->kafkaConsumerBuilder->addConfig($intialConfig);
        $this->kafkaConsumerBuilder->addConfig($newConfig);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['timeout' => 1001, 'offset.store.sync.interval.ms' => 60e3, 'group.id' => 'test-group'], $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetDecoder(): void
    {
        $denormalizer = $this->getMockForAbstractClass(DecoderInterface::class);

        $this->kafkaConsumerBuilder->setDecoder($denormalizer);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'decoder');
        $reflectionProperty->setAccessible(true);

        self::assertInstanceOf(DecoderInterface::class, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetConsumerGroup(): void
    {
        $this->kafkaConsumerBuilder->setConsumerGroup('test-consumer');

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'consumerGroup');
        $reflectionProperty->setAccessible(true);

        self::assertSame('test-consumer', $reflectionProperty->getValue($this->kafkaConsumerBuilder));
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

        self::assertSame(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL, $actualConsumerType->getValue($this->kafkaConsumerBuilder));
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

        self::assertSame(KafkaConsumerBuilder::CONSUMER_TYPE_HIGH_LEVEL, $actualConsumerType->getValue($this->kafkaConsumerBuilder));
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
     * @throws \ReflectionException
     */
    public function testSetConsumeCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $this->kafkaConsumerBuilder->setConsumeCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'consumeCallback');
        $reflectionProperty->setAccessible(true);

        self::assertSame($callback, $reflectionProperty->getValue($this->kafkaConsumerBuilder));
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSetOffsetCommitCallback(): void
    {
        $callback = function () {
            // Anonymous test method, no logic required
        };

        $this->kafkaConsumerBuilder->setOffsetCommitCallback($callback);

        $reflectionProperty = new \ReflectionProperty($this->kafkaConsumerBuilder, 'offsetCommitCallback');
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

        $this->kafkaConsumerBuilder->addBroker('localhost')->build();
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
            ->addBroker('localhost')
            ->addSubscription('test-topic')
            ->setRebalanceCallback($callback)
            ->setOffsetCommitCallback($callback)
            ->setConsumeCallback($callback)
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
            ->addBroker('localhost')
            ->addSubscription('test-topic')
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->setConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL)
            ->build();

        self::assertInstanceOf(KafkaConsumerInterface::class, $consumer);
        self::assertInstanceOf(KafkaLowLevelConsumerInterface::class, $consumer);
    }

    /**
     * @return void
     */
    public function testBuildLowLevelFailureOnUnsupportedCallback(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        self::expectException(KafkaConsumerBuilderException::class);
        self::expectExceptionMessage(
            sprintf(
                KafkaConsumerBuilderException::UNSUPPORTED_CALLBACK_EXCEPTION_MESSAGE,
                'consumerCallback',
                KafkaLowLevelConsumer::class
            )
        );

        $this->kafkaConsumerBuilder
            ->addBroker('localhost')
            ->addSubscription('test-topic')
            ->setConsumeCallback($callback)
            ->setConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL)
            ->build();
    }

    /**
     * @return void
     */
    public function testBuildHighLevelSuccess(): void
    {
        $callback = function ($kafka, $errId, $msg) {
            // Anonymous test method, no logic required
        };

        /** @var $consumer KafkaHighLevelConsumer */
        $consumer = $this->kafkaConsumerBuilder
            ->addBroker('localhost')
            ->addSubscription('test-topic')
            ->setRebalanceCallback($callback)
            ->setErrorCallback($callback)
            ->build();

        self::assertInstanceOf(KafkaConsumerInterface::class, $consumer);
        self::assertInstanceOf(KafkaHighLevelConsumerInterface::class, $consumer);
    }
}
