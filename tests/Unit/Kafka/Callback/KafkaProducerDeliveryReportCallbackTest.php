<?php


namespace Jobcloud\Messaging\Tests\Unit\Kafka\Callback;

use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Message;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;

/**
 * @ Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback
 */
class KafkaProducerDeliveryReportCallbackTest extends TestCase
{

    protected $mockProducer;

    protected $callback;

    public function setUp()
    {
        $this->mockProducer = $this->getMockBuilder(RdKafkaProducer::class)
            ->disableOriginalConstructor()
            ->getMock();

        $this->callback = new KafkaProducerDeliveryReportCallback();
    }

    public function testInvokeDefault()
    {
        self::expectException('Jobcloud\Messaging\Kafka\Exception\KafkaProducerException');

        $message = new Message();
        $message->err = -1;

        call_user_func($this->callback, $this->mockProducer, $message);
    }

    public function testInvokeTimeout()
    {
        self::expectException('Jobcloud\Messaging\Kafka\Exception\KafkaProducerException');

        $message = new Message();
        $message->err = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;

        call_user_func($this->callback, $this->mockProducer, $message);
    }

    public function testInvokeNoError()
    {

        $message = new Message();

        $result = call_user_func($this->callback, $this->mockProducer, $message);

        self::assertNull($result);
    }
}