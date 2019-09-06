<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Decoder;

use Jobcloud\Messaging\Kafka\Message\Decoder\DefaultDecoder;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Decoder\DefaultDecoder
 */
class DefaultDecoderTest extends TestCase
{

    /**
     * @return void
     */
    public function testDenormalize(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);

        self::assertSame($message, (new DefaultDecoder())->decode($message));
    }
}
