<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Encoder;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Encoder\DefaultEncoder;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Encoder\DefaultEncoder
 */
class DefaultEncoderTest extends TestCase
{

    /**
     * @return void
     */
    public function testNormalize(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);

        $this->assertSame($message, (new DefaultEncoder())->encode($message));
    }
}
