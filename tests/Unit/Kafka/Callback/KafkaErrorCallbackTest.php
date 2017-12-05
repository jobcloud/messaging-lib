<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;


use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;

/**
 * @covers \Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback
 */
class KafkaErrorCallbackTest extends TestCase
{

    public function testInvoke()
    {
        self::expectException('Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException');

        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->getMock();

        $callback = new KafkaErrorCallback();
        call_user_func($callback, $consumerMock, 1, 'error');
    }
}