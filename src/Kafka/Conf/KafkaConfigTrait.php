<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Conf;

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

        $conf->set('metadata.broker.list', implode(',', $conf->getBrokers()));

        return $conf;
    }
}
