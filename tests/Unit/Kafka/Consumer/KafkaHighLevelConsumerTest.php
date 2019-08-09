<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumer;

use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerAssignmentException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerRequestException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Message\KafkaMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaMessageInterface;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\AbstractKafkaConsumer
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaHighLevelConsumer
 */
final class KafkaHighLevelConsumerTest extends TestCase
{

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeSuccesss(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::once())->method('getTopicSubscriptions')->willReturn(['test']);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $topics = ['test'];

        $rdKafkaConsumerMock->expects(self::once())->method('subscribe')->with($topics);

        $kafkaConsumer->subscribe($topics);
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testSubscribeFailure(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::once())->method('getTopicSubscriptions')->willReturn(['test']);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $topics = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with($topics)
            ->willThrowException(new RdKafkaException('Error', 100));

        $this->expectException(KafkaConsumerSubscriptionException::class);
        $this->expectExceptionCode(100);
        $this->expectExceptionMessage('Error');


        $kafkaConsumer->subscribe($topics);
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testUnsubscribeSuccesss(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $rdKafkaConsumerMock->expects(self::once())->method('unsubscribe');

        $kafkaConsumer->unsubscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     */
    public function testUnsubscribeFailure(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('unsubscribe')
            ->willThrowException(new RdKafkaException('Error', 100));

        $this->expectException(KafkaConsumerSubscriptionException::class);
        $this->expectExceptionCode(100);
        $this->expectExceptionMessage('Error');


        $kafkaConsumer->unsubscribe();
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitSuccesss(): void
    {
        $key = '1234-1234-1234';
        $body = 'foo bar baz';
        $topic = 'test';
        $offset = 42;
        $partition = 1;
        $timestamp = 1562324233704;
        $headers = [ 'key' => 'value' ];

        $message = new KafkaMessage($key, $body, $topic, $partition, $offset, $timestamp, $headers);

        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);
        $rdKafkaConsumerMock->expects(self::once())->method('commit');

        $kafkaConsumer->commit([$message, $message]);
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitAsyncSuccesss(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);
        $message = $this->createMock(KafkaMessageInterface::class);

        $rdKafkaConsumerMock->expects(self::once())->method('commitAsync');

        $kafkaConsumer->commitAsync([$message]);
    }

    /**
     * @throws KafkaConsumerCommitException
     */
    public function testCommitFails(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);
        $message = $this->createMock(KafkaMessageInterface::class);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('commit')
            ->willThrowException(new RdKafkaException('Failure', 99));

        $this->expectException(KafkaConsumerCommitException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Failure');

        $kafkaConsumer->commit([$message]);
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testAssignSuccess(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $topicPartitions = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('assign')
            ->with($topicPartitions);

        $kafkaConsumer->assign($topicPartitions);
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testAssignFail(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $topicPartitions = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('assign')
            ->with($topicPartitions)
            ->willThrowException(new RdKafkaException('Failure', 99));

        $this->expectException(KafkaConsumerAssignmentException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Failure');

        $kafkaConsumer->assign($topicPartitions);
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testGetAssignment(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $topicPartitions = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getAssignment')
            ->willReturn($topicPartitions);

        $this->assertEquals($topicPartitions, $kafkaConsumer->getAssignment());
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testGetAssignmentException(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getAssignment')
            ->willThrowException(new RdKafkaException('Fail', 99));

        $this->expectException(KafkaConsumerAssignmentException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Fail');
        $kafkaConsumer->getAssignment();
    }

    public function testKafkaConsume(): void
    {
        $message = new Message();
        $message->key = 'test';
        $message->payload = 'test';
        $message->topic_name = 'test';
        $message->partition = 9;
        $message->offset = 500;
        $message->timestamp = 500;
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;

        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with([]);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('consume')
            ->with(0)
            ->willReturn($message);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConfigurationMock->expects(self::once())->method('getTopicSubscriptions')->willReturn([]);
        $kafkaConfigurationMock->expects(self::once())->method('getTimeout')->willReturn(0);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $kafkaConsumer->subscribe();
        $kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testGetCommittedOffsets(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $committedOffsets = ['test'];

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getCommittedOffsets')
            ->with($committedOffsets, 1)
            ->willReturn($committedOffsets);

        $this->assertEquals($committedOffsets, $kafkaConsumer->getCommittedOffsets($committedOffsets, 1));
    }

    /**
     * @throws KafkaConsumerAssignmentException
     */
    public function testGetCommittedOffsetsException(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);

        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getCommittedOffsets')
            ->willThrowException(new RdKafkaException('Fail', 99));

        $this->expectException(KafkaConsumerRequestException::class);
        $this->expectExceptionCode(99);
        $this->expectExceptionMessage('Fail');
        $kafkaConsumer->getCommittedOffsets([], 1);
    }

    /**
     * @return void
     */
    public function testGetOffsetPositions(): void
    {
        $rdKafkaConsumerMock = $this->createMock(RdKafkaHighLevelConsumer::class);
        $kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $kafkaConsumer = new KafkaHighLevelConsumer($rdKafkaConsumerMock, $kafkaConfigurationMock);
        $rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getOffsetPositions')
            ->with([])
            ->willReturn([]);

        $kafkaConsumer->getOffsetPositions([]);
    }
}
