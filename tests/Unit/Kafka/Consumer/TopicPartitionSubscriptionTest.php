<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\TopicPartitionSubscription;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\TopicPartitionSubscription
 */
final class TopicPartitionSubscriptionTest extends TestCase
{
    public function testGettersAndSetters()
    {
        $topicName = 'test';
        $partitionId = 1;
        $offset = 42;

        $topicPartitionSubscription = new TopicPartitionSubscription($topicName);

        self::assertInstanceOf(
            TopicPartitionSubscription::class,
            $topicPartitionSubscription->addPartition($partitionId, $offset)
        );

        self::assertEquals($topicName, $topicPartitionSubscription->getTopicName());
        self::assertEquals([$partitionId => $offset], $topicPartitionSubscription->getPartitions());
    }
}
