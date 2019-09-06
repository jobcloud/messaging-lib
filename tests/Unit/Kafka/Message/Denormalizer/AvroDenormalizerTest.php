<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Denormalizer;

use Jobcloud\Messaging\Kafka\Exception\AvroDenormalizeException;
use Jobcloud\Messaging\Kafka\Message\Denormalizer\AvroDenormalizer;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Denormalizer\AvroDenormalizer
 * @covers \Jobcloud\Messaging\Kafka\Message\Helper\SchemaRegistryHelperTrait
 */
class AvroDenormalizerTest extends TestCase
{
    public function testDenormalizeTombstone()
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(null);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::never())->method('decodeValue');

        $denormalizer = new AvroDenormalizer($transformer, []);

        $result = $denormalizer->denormalize($message);

        self::assertSame($message, $result);
    }

    public function testDenormalizeWithoutSchema()
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::exactly(2))->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::once())->method('getPartition')->willReturn(0);
        $message->expects(self::once())->method('getOffset')->willReturn(1);
        $message->expects(self::once())->method('getTimestamp')->willReturn(time());
        $message->expects(self::once())->method('getKey')->willReturn('test-key');
        $message->expects(self::exactly(3))->method('getBody')->willReturn('body');
        $message->expects(self::once())->method('getHeaders')->willReturn([]);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::once())->method('decodeValue')->with($message->getBody(), null)->willReturn(['test']);

        $denormalizer = new AvroDenormalizer($transformer, []);

        $result = $denormalizer->denormalize($message);

        self::assertInstanceOf(KafkaConsumerMessageInterface::class, $result);
        self::assertSame(json_encode(['test']), $result->getBody());
    }

    public function testDenormalizeWithUnencodableBody()
    {
        self::expectException(\JsonException::class);

        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);
        $message->expects(self::once())->method('getTopicName')->willReturn('test-topic');
        $message->expects(self::never())->method('getPartition');
        $message->expects(self::never())->method('getOffset');
        $message->expects(self::never())->method('getTimestamp');
        $message->expects(self::never())->method('getKey');
        $message->expects(self::exactly(3))->method('getBody')->willReturn('test');
        $message->expects(self::never())->method('getHeaders');

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::once())->method('decodeValue')->with($message->getBody(), null)->willReturn([chr(255)]);

        $denormalizer = new AvroDenormalizer($transformer, []);

        $denormalizer->denormalize($message);
    }
}
