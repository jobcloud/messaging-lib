<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Producer\ProducerInterface;

interface KafkaProducerInterface extends ProducerInterface
{
    /**
     * Returns the schema registry if any was set
     *
     * @return Registry|null
     */
    public function getSchemaRegistry(): ?Registry;

}
