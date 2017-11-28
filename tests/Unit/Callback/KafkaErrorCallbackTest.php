<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;


use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;

/**
 * @covers Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback
 */
class KafkaErrorCallbackTest extends TestCase
{
    /**
     * @expectedException \Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException
     */
    public function testInvoke()
    {
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->getMock();

        $callback = new KafkaErrorCallback();
        call_user_func($callback, $consumerMock, 1, "error");
    }
}