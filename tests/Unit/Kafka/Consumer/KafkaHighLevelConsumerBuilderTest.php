<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilderException;
use Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumerBuilder;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumerBuilder
 */
final class KafkaHighLevelConsumerBuilderTest extends TestCase
{

    /**
     * @var $kcb KafkaHighLevelConsumerBuilder
     */
    protected $kafkaHighLevelConsumerBuilder;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->kafkaHighLevelConsumerBuilder = KafkaHighLevelConsumerBuilder::create();
    }

    /**
     * @return void
     */
    public function testCreate(): void
    {
        self::assertInstanceOf(KafkaHighLevelConsumerBuilder::class, KafkaHighLevelConsumerBuilder::create());
    }

    /**
     * @return void
     */
    public function testAddBroker(): void
    {
        self::assertSame($this->kafkaHighLevelConsumerBuilder, $this->kafkaHighLevelConsumerBuilder->addBroker('localhost'));

        $reflectionProperty = new \ReflectionProperty($this->kafkaHighLevelConsumerBuilder, 'brokers');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['localhost'], $reflectionProperty->getValue($this->kafkaHighLevelConsumerBuilder));
    }

    /**
     * @return void
     */
    public function testSetTimeout(): void
    {
        $timeout = 1000;

        self::assertSame($this->kafkaHighLevelConsumerBuilder, $this->kafkaHighLevelConsumerBuilder->setTimeout($timeout));

        $reflectionProperty = new \ReflectionProperty($this->kafkaHighLevelConsumerBuilder, 'timeout');
        $reflectionProperty->setAccessible(true);

        self::assertSame($timeout, $reflectionProperty->getValue($this->kafkaHighLevelConsumerBuilder));
    }

    /**
     * @return void
     */
    public function testSetConfig(): void
    {
        $this->kafkaHighLevelConsumerBuilder->setConfig(
            [
              'timeout' => 100
            ]
        );

        $reflectionProperty = new \ReflectionProperty($this->kafkaHighLevelConsumerBuilder, 'config');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['timeout' => 100], $reflectionProperty->getValue($this->kafkaHighLevelConsumerBuilder));
    }

    /**
     * @return void
     */
    public function testSetConsumerGroup(): void
    {
        $this->kafkaHighLevelConsumerBuilder->setConsumerGroup('funGroup');

        $reflectionProperty = new \ReflectionProperty($this->kafkaHighLevelConsumerBuilder, 'consumerGroup');
        $reflectionProperty->setAccessible(true);

        self::assertSame('funGroup', $reflectionProperty->getValue($this->kafkaHighLevelConsumerBuilder));
    }

    /**
     * @return void
     */
    public function testBuildFail(): void
    {
        self::expectException(KafkaConsumerBuilderException::class);

        $this->kafkaHighLevelConsumerBuilder
            ->setConfig(['group.id' => 'foo'])
            ->build();
    }

    /**
     * @return void
     */
    public function testBuildSuccess(): void
    {
        $consumer = KafkaHighLevelConsumerBuilder::create()
            ->addBroker('localhost')
            ->addSubscription(new TopicSubscription('TEST_TOPIC'))
            ->build();

        self::assertInstanceOf(KafkaConsumer::class, $consumer);
    }
}
