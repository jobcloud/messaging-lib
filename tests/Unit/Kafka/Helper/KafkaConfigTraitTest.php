<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Helper;

use Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait
 */
class KafkaConfigTraitTest extends TestCase
{
    private const TEST_CONFIGURATION_NAME = 'group.id';
    private const TEST_CONFIGURATION_VALUE = 'TEST_CONFIGURATION_VALUE';

    /**
     * @return void
     */
    public function testCreateKafkaConfig(): void
    {
        $anonymousConfigTraitUserClass = new class
        {
            use KafkaConfigTrait;

            public function createTraitKafkaConfig(array $config): KafkaConfiguration
            {
                return $this->createKafkaConfig($config, [], [], 0);
            }
        };

        /** @var KafkaConfiguration $config */
        $config = $anonymousConfigTraitUserClass->createTraitKafkaConfig(
            [
                self::TEST_CONFIGURATION_NAME => self::TEST_CONFIGURATION_VALUE
            ]
        );
        self::assertInstanceOf(KafkaConfiguration::class, $config);

        $configArray = $config->dump();
        self::assertTrue(isset($configArray[self::TEST_CONFIGURATION_NAME]));
        self::assertEquals($configArray[self::TEST_CONFIGURATION_NAME], self::TEST_CONFIGURATION_VALUE);
    }
}
