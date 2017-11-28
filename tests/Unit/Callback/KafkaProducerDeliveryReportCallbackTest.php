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
    /**
     * @expectedException \Jobcloud\Messaging\Kafka\Exception\KafkaProducerException
     */
    public function testInvoke()
    {
        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->disableOriginalConstructor()
            ->getMock();
        $message = new Message();
        $message->err = -1;

        $callback = new KafkaProducerDeliveryReportCallback();
        call_user_func($callback, $producerMock, $message);
    }
}