<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Encoder;

use Jobcloud\Messaging\Kafka\Exception\AvroNormalizerException;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;
use PHPStan\Testing\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder
 */
class AvroEncoderTest extends TestCase
{
    public function testNormalizeTombstone()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getBody')->willReturn(null);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::never())->method('encodeValue');
        $normalizer = new AvroEncoder($transformer);
        $result = $normalizer->encode($producerMessage);

        self::assertSame($producerMessage, $result);
    }

    public function testNormalizeWithoutSchema()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(3))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn('test');


        self::expectException(AvroNormalizerException::class);
        self::expectExceptionMessage(
            sprintf(
                AvroNormalizerException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                $producerMessage->getTopicName()
            )
        );

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $normalizer = new AvroEncoder($transformer);
        $normalizer->encode($producerMessage);
    }

    public function testNormalizeWithoutJsonBody()
    {
        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn('test');

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getSchemaForTopic')->willReturn($avroSchema);

        self::expectException(AvroNormalizerException::class);
        self::expectExceptionMessage(AvroNormalizerException::MESSAGE_BODY_MUST_BE_JSON_MESSAGE);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::once())->method('getSchemaRegistry')->willReturn($registry);

        $normalizer = new AvroEncoder($transformer);
        $normalizer->encode($producerMessage);
    }

    public function testNormalizeSuccessWithoutSchema()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(2))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::never())->method('getVersion');
        $avroSchema->expects(self::exactly(2))->method('getDefinition')->willReturn($schemaDefinition);

        $registry = $this->getMockForAbstractClass(AvroSchemaRegistryInterface::class);
        $registry->expects(self::once())->method('getSchemaForTopic')->willReturn($avroSchema);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn('{}');
        $producerMessage->expects(self::once())->method('withBody')->with('encodedValue')->willReturn($producerMessage);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::once())->method('getSchemaRegistry')->willReturn($registry);
        $transformer->expects(self::once())->method('encodeValue')->with($avroSchema->getName(), $avroSchema->getDefinition(), [])->willReturn('encodedValue');
        $transformer->expects(self::once())->method('getSchemaRegistry')->willReturn($registry);

        $normalizer = new AvroEncoder($transformer);

        self::assertSame($producerMessage, $normalizer->encode($producerMessage));
    }
}
