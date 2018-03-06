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
        $offsetCommitInterval = 1100;

        $topicSubscription = new TopicSubscription($topicName, $defaultOffset, $offsetCommitInterval);

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
        $topicConf = $topicSubscription->getTopicConf();
        $config = $topicConf->dump();
        self::assertArraySubset(
            [
                'auto.commit.enable' => 'false',
                'auto.commit.interval.ms' => '1100'
            ],
            $config
        );
    }
}
