<?php

namespace Jobcloud\Messaging\Kafka\Helper;

use Jobcloud\Messaging\Kafka\KafkaConfiguration;

trait KafkaConfigTrait
{
    /**
     * @param array   $config
     * @param array   $brokers
     * @param array   $topicSubscriptions
     * @param integer $timeout
     * @return KafkaConfiguration
     */
    protected function createKafkaConfig(
        array $config,
        array $brokers,
        array $topicSubscriptions,
        int $timeout
    ): KafkaConfiguration {
        $conf = new KafkaConfiguration($brokers, $topicSubscriptions, $timeout);

        foreach ($config as $name => $value) {
            $conf->set($name, is_bool($value) ? (false === $value ? 'false' : 'true') : $value);
        }

        return $conf;
    }
}
