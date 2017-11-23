<?php

namespace Jobcloud\Kafka\Helper;

use RdKafka\Conf;

trait ConfigTrait {

    /**
     * @param array $config
     * @return Conf
     */
    protected function getConfig(array $config): Conf
    {
        $defaultConfig = [];
        $config += $defaultConfig;
        $conf = $this->createConfig($config);

        return $conf;
    }

    /**
     * @param array $config
     * @return Conf
     */
    protected function createConfig(array $config): Conf
    {
        $conf = new Conf();

        foreach ($config as $name => $value) {
            $conf->set($name, $value);
        }

        return $conf;
    }
}