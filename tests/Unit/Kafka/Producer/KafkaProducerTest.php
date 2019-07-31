<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducer
 */
class KafkaProducerTest extends TestCase
{
    /** @var int */
    private const TEST_TIMEOUT = 9999;
    /** @var string */
    private const TEST_BROKER = 'TEST_BROKER';
    /** @var string */
    private const TEST_TOPIC = 'TEST_TOPIC';

    /** @var KafkaConfiguration|MockObject */
    private $kafkaConfigurationMock;

    /** @var RdKafkaProducer|MockObject */
    private $rdKafkaProducerMock;

    /** @var KafkaProducer */
    private $kafkaProducer;

    public function setUp(): void
    {
        $this->kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $this->rdKafkaProducerMock = $this->createMock(RdKafkaProducer::class);
        $this->kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock);
    }

    /**
     * @return void
     */
    public function testProduceError(): void
    {
        self::expectException(KafkaProducerException::class);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('produce')
            ->with(RD_KAFKA_PARTITION_UA, 0, 'test')
            ->willThrowException(new KafkaProducerException());

        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getBrokers')
            ->willReturn([self::TEST_BROKER]);
        $this->rdKafkaProducerMock
            ->expects(self::any())
            ->method('addBrokers')
            ->with(self::TEST_BROKER);
        $this->rdKafkaProducerMock
            ->expects(self::any())
            ->method('newTopic')
            ->willReturn($rdKafkaProducerTopicMock);

        $this->kafkaProducer->produce('test', 'test');
    }

    public function testProduceSuccess()
    {
        $expectedMessage1 = 'message1';
        $expectedMessage2 = 'message2';
        $expectedKey2 = 'foopass';
        $expectedPartition1 = 1;
        $expectedPartition2 = 2;
        $expectedFlags = 0;

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::exactly(2))
            ->method('produce')
            ->willReturnCallback(
                function (
                    $partition,
                    $flags,
                    $message,
                    $key
                ) use (
                    $expectedMessage1,
                    $expectedMessage2,
                    $expectedKey2,
                    $expectedPartition1,
                    $expectedPartition2,
                    $expectedFlags
                ) {
                    self::assertEquals($expectedFlags, $flags);

                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                            self::assertEquals($expectedMessage1, $message);
                            self::assertEquals($expectedPartition1, $partition);
                            self::assertNull($key);

                            return;
                        case 1:
                            self::assertEquals($expectedMessage2, $message);
                            self::assertEquals($expectedPartition2, $partition);
                            self::assertEquals($expectedKey2, $key);

                            return;
                        default:
                            self::assertFileEquals('nonExistingMessage', $message);
                    }
                }
            );

        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getBrokers')
            ->willReturn([self::TEST_BROKER]);
        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(self::TEST_TIMEOUT);
        $this->rdKafkaProducerMock
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
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with(self::TEST_TOPIC)
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(self::TEST_TIMEOUT);
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('addBrokers')
            ->with(self::TEST_BROKER);

        $this->kafkaProducer->produce($expectedMessage1, self::TEST_TOPIC, $expectedPartition1);
        $this->kafkaProducer->produce($expectedMessage2, self::TEST_TOPIC, $expectedPartition2, $expectedKey2);
    }
}
