<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\KafkaConfiguration;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\KafkaConfiguration
 */
class KafkaConfigurationTest extends TestCase
{
    /** @var int */
    private const TEST_TIMEOUT = 99999;
    private const TEST_BROKER = 'TEST_BROKER';
    private const TEST_TOPIC_NAME = 'TEST_TOPIC_NAME';
    private const TEST_VALID_CONFIGURATION = 'group.id';
    private const TEST_VALID_CONFIGURATION_VALUE = 'TEST_VALID_CONFIGURATION_VALUE';
    private const TEST_INVALID_CONFIGURATION = 'configuration.test';

    /**
     * @return void
     */
    public function testInstance(): void
    {
        self::assertInstanceOf(KafkaConfiguration::class, new KafkaConfiguration([], [], self::TEST_TIMEOUT));
    }

    /**
     * @return array
     */
    public function kafkaConfigurationDataProvider(): array
    {
        $brokers = [self::TEST_BROKER];
        $topicSubscriptions = [new TopicSubscription(self::TEST_TOPIC_NAME)];

        return [
            [
                $brokers,
                $topicSubscriptions
            ]
        ];
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGettersAndSetters(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, self::TEST_TIMEOUT);

        self::assertEquals($brokers, $kafkaConfiguration->getBrokers());
        self::assertEquals($topicSubscriptions, $kafkaConfiguration->getTopicSubscriptions());
        self::assertEquals(self::TEST_TIMEOUT, $kafkaConfiguration->getTimeout());
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetValidConfiguration(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, self::TEST_TIMEOUT);
        $kafkaConfiguration->set(self::TEST_VALID_CONFIGURATION, self::TEST_VALID_CONFIGURATION_VALUE);

        self::assertEquals(
            self::TEST_VALID_CONFIGURATION_VALUE,
            $kafkaConfiguration->getConfiguration(self::TEST_VALID_CONFIGURATION)
        );
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetInvalidConfiguration(array $brokers, array $topicSubscriptions): void
    {
        self::expectException(\InvalidArgumentException::class);

        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, self::TEST_TIMEOUT);
        $kafkaConfiguration->getConfiguration(self::TEST_INVALID_CONFIGURATION);
    }
}
