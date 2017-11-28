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
    protected $callback;

    /**
     * @var RdKafkaConsumer
     */
    protected $consumerMock;

    protected $assignments;

    public function setUp()
    {
        //callback for topic assignment
        $callback = function () {
            $args = func_get_args();

            $this->assignments = $args[0];
        };

        //create mock to assign topics
        $this->consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->disableOriginalConstructor()
            ->setMethods(['assign'])
            ->getMock();

        $this->consumerMock
            ->expects(self::any())
            ->method('assign')
            ->willReturnCallback($callback);

        $this->callback = new KafkaConsumerRebalanceCallback();
    }

    /**
     * @expectedException \Jobcloud\Messaging\Kafka\Exception\KafkaRebalanceException
     */
    public function testInvokeWithError()
    {

        call_user_func($this->callback, $this->consumerMock, 1, []);
    }

    public function testInvokeAssign()
    {
        call_user_func($this->callback, $this->consumerMock, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, ['test']);
        self::assertEquals(['test'], $this->assignments);
    }

    public function testInvokeRevoke()
    {
        call_user_func($this->callback, $this->consumerMock, RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS);
        self::assertEquals(null, $this->assignments);
    }
}