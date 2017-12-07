<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback;
use Jobcloud\Messaging\Kafka\Exception\KafkaRebalanceException;
use PHPUnit\Framework\MockObject\MockObject;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback
 */
class KafkaConsumerRebalanceCallbackTest extends TestCase
{
    public function testInvokeWithError()
    {
        self::expectException(KafkaRebalanceException::class);

        $consumer = $this->getConsumerMock(function () {
            self::assertEquals(null, func_get_args()[0]);
        });

        call_user_func(new KafkaConsumerRebalanceCallback(), $consumer, 1, []);
    }

    public function testInvokeAssign()
    {
        $consumer = $this->getConsumerMock(function () {
            self::assertEquals(['test'], func_get_args()[0]);
        });

        call_user_func(new KafkaConsumerRebalanceCallback(), $consumer, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, ['test']);
    }

    public function testInvokeRevoke()
    {
        $consumer = $this->getConsumerMock(function () {
            self::assertEquals(null, func_get_args()[0]);
        });

        call_user_func(new KafkaConsumerRebalanceCallback(), $consumer, RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS);
    }

    /**
     * @var MockObject|RdKafkaConsumer
     */
    private function getConsumerMock(callable $callback)
    {
        //create mock to assign topics
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->setMethods(['assign'])
            ->getMock();

        $consumerMock
            ->expects(self::any())
            ->method('assign')
            ->willReturnCallback($callback);

        return $consumerMock;
    }
}
