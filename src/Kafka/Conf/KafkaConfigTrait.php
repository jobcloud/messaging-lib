<?php

namespace Jobcloud\Messaging\Kafka\Conf;

use RdKafka\TopicConf;

trait KafkaConfigTrait
{
    /**
     * @param array   $config
     * @param array   $topicConfig
     * @param array   $brokers
     * @param array   $topicSubscriptions
     * @param integer $timeout
     * @return KafkaConfiguration
     */
    protected function createKafkaConfig(
        array $config,
        array $topicConfig,
        array $brokers,
        array $topicSubscriptions,
        int $timeout
    ): KafkaConfiguration {
        $conf = new KafkaConfiguration($brokers, $topicSubscriptions, $timeout);

        foreach ($config as $name => $value) {
            $conf->set($name, is_bool($value) ? (false === $value ? 'false' : 'true') : $value);
        }

        $conf->set('metadata.broker.list', implode(',', $conf->getBrokers()));

        if ([] === $topicConfig) {
            return $conf;
        }

        $topicConf = new TopicConf();

        foreach ($topicConfig as $name => $value) {
            $topicConf->set($name, is_bool($value) ? (false === $value ? 'false' : 'true') : $value);
        }

        $conf->setDefaultTopicConf($topicConf);

        return $conf;
    }
}
