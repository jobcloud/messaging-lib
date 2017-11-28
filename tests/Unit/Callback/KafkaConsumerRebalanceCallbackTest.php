<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use PHPUnit\Framework\TestCase;

/**
 * @covers Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback
 */
class KafkaConsumerRebalanceCallbackTest extends TestCase
{
    /**
     * @expectedException \Jobcloud\Messaging\Kafka\Exception\KafkaRebalanceException
     */
    public function testInvoke()
    {
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->getMock();

        $callback = new KafkaConsumerRebalanceCallback();
        call_user_func($callback, $consumerMock, 1, []);
    }
}