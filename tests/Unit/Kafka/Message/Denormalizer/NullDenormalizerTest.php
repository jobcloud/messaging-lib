<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Denormalizer;

use Jobcloud\Messaging\Kafka\Message\Denormalizer\NullDenormalizer;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Denormalizer\NullDenormalizer
 */
class NullDenormalizerTest extends TestCase
{

    /**
     * @return void
     */
    public function testDenormalize(): void
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);

        self::assertSame($message, (new NullDenormalizer())->denormalize($message));
    }
}
