<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration
 */
class KafkaConfigurationTest extends TestCase
{

    /**
     * @return void
     */
    public function testInstance(): void
    {
        self::assertInstanceOf(KafkaConfiguration::class, new KafkaConfiguration([], [], 1000));
    }

    /**
     * @return array
     */
    public function kafkaConfigurationDataProvider(): array
    {
        $brokers = ['localhost'];
        $topicSubscriptions = [new TopicSubscription('test-topic')];

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
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, 1000);

        self::assertEquals($brokers, $kafkaConfiguration->getBrokers());
        self::assertEquals($topicSubscriptions, $kafkaConfiguration->getTopicSubscriptions());
        self::assertEquals(1000, $kafkaConfiguration->getTimeout());
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetConfiguration(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, 1000);

        self::assertEquals($kafkaConfiguration->dump(), $kafkaConfiguration->getConfiguration());
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetValidSetting(array $brokers, array $topicSubscriptions): void
    {
        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, 1000);
        $kafkaConfiguration->set('group.id', 'test-group');

        self::assertEquals(
            'test-group',
            $kafkaConfiguration->getSetting('group.id')
        );
    }

    /**
     * @dataProvider kafkaConfigurationDataProvider
     * @param array $brokers
     * @param array $topicSubscriptions
     * @return void
     */
    public function testGetInvalidSetting(array $brokers, array $topicSubscriptions): void
    {
        self::expectException(\InvalidArgumentException::class);

        $kafkaConfiguration = new KafkaConfiguration($brokers, $topicSubscriptions, 1000);
        $kafkaConfiguration->getSetting('some.none.existing.config');
    }
}
