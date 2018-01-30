<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\TopicPartitionSubscription;
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
        $offset = 42;

        $topicPartitionSubscription = new TopicSubscription($topicName, $offset);

        self::assertEquals($topicName, $topicPartitionSubscription->getTopicName());
        self::assertEquals($offset, $topicPartitionSubscription->getOffset());
    }
}
