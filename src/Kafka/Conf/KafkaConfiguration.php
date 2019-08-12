<?php

namespace Jobcloud\Messaging\Kafka\Conf;

use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
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
     * @param string $name
     * @return string
     * @throws \InvalidArgumentException
     */
    public function getSetting(string $name): string
    {
        $configuration = $this->dump();
        if (isset($configuration[$name])) {
            return $configuration[$name];
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
