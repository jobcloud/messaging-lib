<?php

namespace Jobcloud\Messaging\Kafka\Helper;

use RdKafka\Conf;

trait KafkaConfigTrait {
    /**
     * @param array $config
     * @return Conf
     */
    protected function createKafkaConfig(array $config): Conf
    {
        $conf = new Conf();

        foreach ($config as $name => $value) {
            $conf->set($name, $value);
        }

        return $conf;
    }
}