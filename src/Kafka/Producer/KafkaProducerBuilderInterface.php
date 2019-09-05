<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Message\Normalizer\NormalizerInterface;
use Jobcloud\Messaging\Producer\ProducerInterface;

interface KafkaProducerBuilderInterface
{
    /**
     * @return ProducerInterface
     */
    public function build(): ProducerInterface;

    /**
     * @return KafkaProducerBuilderInterface
     */
    public static function create(): self;

    /**
     * @param string $broker
     * @return KafkaProducerBuilderInterface
     */
    public function addBroker(string $broker): self;

    /**
     * @param array $config
     * @return KafkaProducerBuilderInterface
     */
    public function addConfig(array $config): self;

    /**
     * @param callable $deliveryReportCallback
     * @return KafkaProducerBuilderInterface
     */
    public function setDeliveryReportCallback(callable $deliveryReportCallback): self;

    /**
     * @param callable $errorCallback
     * @return KafkaProducerBuilderInterface
     */
    public function setErrorCallback(callable $errorCallback): self;

    /**
     * @param integer $pollTimeout
     * @return KafkaProducerBuilderInterface
     */
    public function setPollTimeout(int $pollTimeout): self;

    /**
     * Lets you set a custom normalizer for produce message
     *
     * @param NormalizerInterface $normalizer
     * @return KafkaProducerBuilderInterface
     */
    public function setNormalizer(NormalizerInterface $normalizer): self;
}
