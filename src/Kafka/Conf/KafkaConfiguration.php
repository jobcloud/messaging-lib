<?php

namespace Jobcloud\Messaging\Kafka\Conf;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use RdKafka\Conf as RdKafkaConf;

class KafkaConfiguration extends RdKafkaConf
{

    /** @var array */
    protected $brokers;

    /** @var array */
    protected $topicSubscriptions;

    /** @var int */
    protected $timeout;

    /**
     * @param array $brokers
     * @param array $topicSubscriptions
     * @param integer $timeout
     * @param array $config
     */
    public function __construct(array $brokers, array $topicSubscriptions, int $timeout, array $config = [])
    {
        parent::__construct();

        $this->brokers = $brokers;
        $this->topicSubscriptions = $topicSubscriptions;
        $this->timeout = $timeout;

        $this->initializeConfig($config);
    }

    /**
     * @return array
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return array|TopicSubscription[]
     */
    public function getTopicSubscriptions(): array
    {
        return $this->topicSubscriptions;
    }

    /**
     * @return integer
     */
    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * @return array
     */
    public function getConfiguration(): array
    {
        return $this->dump();
    }

    /**
     * @param array $config
     * @return void
     */
    protected function initializeConfig(array $config = []): void
    {

        foreach ($config as $name => $value) {
            // If user pass callable, why not, we will call it, and receive result
            if (true === is_callable($value)) {
                $value = $value();
            }

            // First, we skip if user set object or array in config,
            // because we on't have plan for that scenario
            if (true === is_object($value) || true === is_array($value)) {
                continue;
            }

            // Let's manually cast if it's bool, to string
            if (true === is_bool($value)) {
                $value = true === $value ? 'true' : 'false';
            }

            $this->set($name, (string) $value);
        }

        $this->set('metadata.broker.list', implode(',', $this->getBrokers()));
    }
}
