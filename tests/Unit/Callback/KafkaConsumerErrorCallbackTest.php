<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;


use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerErrorCallback;

/**
 * @covers Jobcloud\Messaging\Kafka\Callback\KafkaConsumerErrorCallback
 */
class KafkaConsumerErrorCallbackTest extends TestCase
{
    /**
     * @expectedException \Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException
     */
    public function testInvoke()
    {
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->getMock();

        $callback = new KafkaConsumerErrorCallback();
        call_user_func($callback, $consumerMock, 1, "error");
    }
}