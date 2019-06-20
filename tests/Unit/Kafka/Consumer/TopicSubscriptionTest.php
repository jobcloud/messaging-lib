<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use PHPUnit\Framework\TestCase;
use RdKafka\TopicConf;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\TopicSubscription
 */
final class TopicSubscriptionTest extends TestCase
{
    public function testGettersAndSetters()
    {
        $topicName = 'test';
        $partitionId = 1;
        $otherPartitionId = 2;
        $offset = 42;
        $defaultOffset = 1;

        $topicSubscription = new TopicSubscription($topicName, $defaultOffset);

        self::assertSame($topicSubscription, $topicSubscription->addPartition($partitionId, $offset));
        self::assertSame($topicSubscription, $topicSubscription->addPartition($otherPartitionId));
        self::assertEquals($topicName, $topicSubscription->getTopicName());
        self::assertEquals(
            [$partitionId => $offset, $otherPartitionId => $defaultOffset],
            $topicSubscription->getPartitions()
        );

        self::assertEquals($defaultOffset, $topicSubscription->getPartitions()[$otherPartitionId]);
        self::assertEquals($defaultOffset, $topicSubscription->getDefaultOffset());
        self::assertInstanceOf(TopicConf::class, $topicSubscription->getTopicConf());
    }

    public function testDefaultSettings()
    {
        $topicSubscription = new TopicSubscription('test');

        $reflectionProperty = new \ReflectionProperty($topicSubscription, 'topicSettings');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['auto.offset.reset' => 'smallest'], $reflectionProperty->getValue($topicSubscription));
    }

    public function testOverrideSettings()
    {
        $topicSubscription = new TopicSubscription('test', RD_KAFKA_OFFSET_STORED, ['auto.offset.reset' => 'latest']);

        $reflectionProperty = new \ReflectionProperty($topicSubscription, 'topicSettings');
        $reflectionProperty->setAccessible(true);

        self::assertSame(['auto.offset.reset' => 'latest'], $reflectionProperty->getValue($topicSubscription));
    }

    public function testAppendSettings()
    {
        $topicSubscription = new TopicSubscription('test', RD_KAFKA_OFFSET_STORED, ['request.required.acks' => '1']);

        $reflectionProperty = new \ReflectionProperty($topicSubscription, 'topicSettings');
        $reflectionProperty->setAccessible(true);

        self::assertSame(
            ['request.required.acks' => '1', 'auto.offset.reset' => 'smallest'],
            $reflectionProperty->getValue($topicSubscription)
        );
    }
}
