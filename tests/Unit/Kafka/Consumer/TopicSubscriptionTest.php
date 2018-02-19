<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use PHPUnit\Framework\TestCase;

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
    }
}
