<?php

declare(strict_types=1);

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
}
