<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\Queue;
use RdKafka\TopicPartition;

final class KafkaLowLevelConsumer extends AbstractKafkaConsumer implements KafkaLowLevelConsumerInterface
{

    /** @var array|ConsumerTopic[] */
    protected $topics = [];

    /** @var Queue */
    protected $queue;

    /**
     * @param RdKafkaLowLevelConsumer $consumer
     * @param KafkaConfiguration      $kafkaConfiguration
     */
    public function __construct(RdKafkaLowLevelConsumer $consumer, KafkaConfiguration $kafkaConfiguration)
    {
        $this->consumer = $consumer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->queue = $consumer->newQueue();
    }

    /**
     * Tries to subscribe to the given topics and returns a list of successfully subscribed topics
     * @return void
     * @throws KafkaConsumerSubscriptionException
     */
    public function subscribe(): void
    {
        if (true === $this->isSubscribed()) {
            return;
        }

        $this->connectConsumerToBrokers();

        try {
            $topicSubscriptions = $this->getConfiguration()->getTopicSubscriptions();
            foreach ($topicSubscriptions as $topicSubscription) {
                $topicName = $topicSubscription->getTopicName();

                if (false === isset($this->topics[$topicName])) {
                    $this->topics[$topicName] = $topic = $this->consumer->newTopic(
                        $topicName,
                        $topicSubscription->getTopicConf()
                    );

                    //Check partition subscription, if not set, subscribe to all partitions
                    $this->verifyAndSetPartitionSubcriptions($topic, $topicSubscription);
                } else {
                    $topic = $this->topics[$topicName];
                }

                foreach ($topicSubscription->getPartitions() as $partitionId => $offset) {
                    $topic->consumeQueueStart($partitionId, $offset, $this->queue);
                }
            }

            $this->subscribed = true;
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param MessageInterface[]|MessageInterface $messages
     * @return void
     * @throws KafkaConsumerCommitException
     */
    public function commit($messages): void
    {
        $messages = is_array($messages) ? $messages : [$messages];

        foreach ($messages as $i => $message) {
            if (false === $message instanceof Message) {
                throw new KafkaConsumerCommitException(
                    sprintf('Provided message (index: %d) is not an instance of "%s"', $i, Message::class)
                );
            }

            $this->topics[$message->getTopicName()]->offsetStore(
                $message->getPartition(),
                $message->getOffset()
            );
        }
    }

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return void
     */
    public function unsubscribe(): void
    {
        if (false === $this->isSubscribed()) {
            return;
        }

        $topicSubscriptions = $this->getConfiguration()->getTopicSubscriptions();

        /** @var TopicSubscription $topicSubscription */
        foreach ($topicSubscriptions as $topicSubscription) {
            foreach ($topicSubscription->getPartitions() as $partitionId => $offset) {
                $this->topics[$topicSubscription->getTopicName()]->consumeStop($partitionId);
            }
        }

        $this->subscribed = false;
    }

    /**
     * @param ConsumerTopic     $topic
     * @param TopicSubscription $topicSubscription
     * @throws RdKafkaException
     * @return void
     */
    private function verifyAndSetPartitionSubcriptions(ConsumerTopic $topic, TopicSubscription $topicSubscription): void
    {
        if ([] === $topicSubscription->getPartitions()) {
            $topicMetadata = $this->getMetadataForTopic($topic);

            foreach ($topicMetadata->getPartitions() as $partition) {
                $topicSubscription->addPartition(
                    $partition->getId(),
                    $topicSubscription->getDefaultOffset()
                );
            }
        }
    }

    /**
     * @param array|TopicPartition[] $topicPartitions
     * @param integer                $timeout
     * @return array
     */
    public function offsetsForTimes(array $topicPartitions, int $timeout): array
    {
        return $this->consumer->offsetsForTimes($topicPartitions, $timeout);
    }

    /**
     * @param string  $topic
     * @param integer $partition
     * @param integer $lowOffset
     * @param integer $highOffset
     * @param integer $timeout
     * @return void
     */
    public function getBrokerHighLowOffsets(
        string $topic,
        int $partition,
        int &$lowOffset,
        int &$highOffset,
        int $timeout
    ): void {
        $this->consumer->queryWatermarkOffsets($topic, $partition, $lowOffset, $highOffset, $timeout);
    }

    /**
     * @param integer $timeout
     * @return null|RdKafkaMessage
     */
    protected function kafkaConsume(int $timeout): ?RdKafkaMessage
    {
        return $this->queue->consume($timeout);
    }
}
