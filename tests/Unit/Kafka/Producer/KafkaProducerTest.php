<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use PHPUnit\Framework\MockObject\MockObject;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use RdKafka\Producer as RdKafkaProducer;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducer
 */
class KafkaProducerTest extends TestCase
{

    public function testProduceError()
    {
        self::expectException(KafkaProducerException::class);

        $producerTopicMock = $this->getMockBuilder(RdKafkaProducerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();

        $producerTopicMock
            ->expects(self::once())
            ->method('produce')
            ->with(RD_KAFKA_PARTITION_UA, 0, 'test')
            ->willThrowException(new KafkaProducerException());

        /** @var MockObject|RdKafkaProducer $producerMock */
        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->setMethods(['newTopic', 'addBrokers'])
            ->disableOriginalConstructor()
            ->getMock();

        $producerMock
            ->expects(self::any())
            ->method('addBrokers')
            ->with('localhost');

        $producerMock
            ->expects(self::any())
            ->method('newTopic')
            ->willReturn($producerTopicMock);


        $producer = new KafkaProducer($producerMock, ['localhost'], 0);

        $producer->produce('test', 'test');
    }

    public function testProduceSuccess()
    {
        $expectedMessage1 = 'message1';
        $expectedMessage2 = 'message2';
        $expectedPartition1 = 1;
        $expectedPartition2 = 2;
        $expectedTopic = 'topic';
        $expectedFlags = 0;
        $expectedTimeout = 10;

        $producerTopicMock = $this
            ->getMockBuilder(RdKafkaProducerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['produce'])
            ->getMock();

        $producerTopicMock
            ->expects(self::exactly(2))
            ->method('produce')
            ->willReturnCallback(
                function (
                    $partition,
                    $flags,
                    $message
                ) use (
                    $expectedMessage1,
                    $expectedMessage2,
                    $expectedPartition1,
                    $expectedPartition2,
                    $expectedTopic,
                    $expectedFlags
                ) {
                    self::assertEquals($expectedFlags, $flags);

                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                            self::assertEquals($expectedMessage1, $message);
                            self::assertEquals($expectedPartition1, $partition);

                            return;
                        case 1:
                            self::assertEquals($expectedMessage2, $message);
                            self::assertEquals($expectedPartition2, $partition);

                            return;
                        default:
                            self::assertFileEquals('nonExistingMessage', $message);
                    }
                }
            );

        $producerMock = $this->getRdKafkaProducer();

        $producerMock
            ->expects(self::exactly(4))
            ->method('getOutQLen')
            ->willReturnCallback(
              function () {
                  static $messageCount = 0;
                  switch ($messageCount++) {
                      case 0:
                      case 1:
                          return 1;
                      default:
                          return 0;
                  }
              }
            );

        $producerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($expectedTopic)
            ->willReturn($producerTopicMock);

        $producerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with($expectedTimeout);

        $producerMock
            ->expects(self::once())
            ->method('addBrokers')
            ->with('localhost');

        $producer = new KafkaProducer($producerMock, ['localhost'], $expectedTimeout);

        $producer->produce($expectedMessage1, $expectedTopic, $expectedPartition1);
        $producer->produce($expectedMessage2, $expectedTopic, $expectedPartition2);
    }

    /**
     * @return MockObject|RdKafkaProducer $producerMock
     */
    private function getRdKafkaProducer(): RdKafkaProducer
    {
        $producerMock = $this->getMockBuilder(RdKafkaProducer::class)
            ->setMethods(['newTopic', 'addBrokers', 'poll', 'getOutQLen'])
            ->disableOriginalConstructor()
            ->getMock();

        return $producerMock;
    }
}
