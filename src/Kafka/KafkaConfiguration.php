<?php

namespace Jobcloud\Messaging\Kafka;

use RdKafka\Conf;

class KafkaConfiguration extends Conf
{

    /** @var array */
    protected $brokers;

    /** @var array */
    protected $topicSubscriptions;

    /** @var int */
    protected $timeout;

    /**
     * @param array   $brokers
     * @param array   $topicSubscriptions
     * @param integer $timeout
     */
    public function __construct(array $brokers, array $topicSubscriptions, int $timeout)
    {
        parent::__construct();

        $this->brokers = $brokers;
        $this->topicSubscriptions = $topicSubscriptions;
        $this->timeout = $timeout;
    }

    /**
     * @return array
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return array
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
     * @param string $name
     * @return string
     * @throws \InvalidArgumentException
     */
    public function getConfiguration(string $name): string
    {
        $configuration = $this->dump();
        foreach ($configuration as $configurationKey => $configurationValue) {
            if ($configurationKey === $name) {
                return $configurationValue;
            }
        }

        throw new \InvalidArgumentException(
            sprintf(
                'Configuration name `%s` does not exists on class `%s`. Available options are: %s.',
                $name,
                KafkaConfiguration::class,
                implode(', ', array_keys($configuration))
            )
        );
    }
}
