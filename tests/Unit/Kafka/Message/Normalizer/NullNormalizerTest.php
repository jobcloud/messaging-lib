<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Normalizer;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Normalizer\NullNormalizer;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Normalizer\NullNormalizer
 */
class NullNormalizerTest extends TestCase
{

    /**
     * @return void
     */
    public function testNormalize(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);

        $this->assertSame($message, (new NullNormalizer())->normalize($message));
    }
}
