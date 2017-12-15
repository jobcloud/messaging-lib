<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

final class ProducerPool implements ProducerInterface
{

    /**
     * @var ProducerInterface[]
     */
    private $producers;

    /**
     * @param ProducerInterface[] $producers
     */
    public function __construct(array $producers = [])
    {
        $this->producers = $producers;
    }

    /**
     * @param string $message
     * @param string $topic
     * @param int    $partition
     * @return void
     */
    public function produce(string $message, string $topic, int $partition = RD_KAFKA_PARTITION_UA)
    {
        foreach ($this->producers as $producer) {
            $producer->produce($message, $topic, $partition);
        }
    }

    /**
     * @return array
     */
    public function getProducerPool(): array
    {
        return $this->producers;
    }

    /**
     * @param ProducerInterface $producer
     * @return ProducerPool
     */
    public function addProducer(ProducerInterface $producer): self
    {
        $this->producers[] = $producer;

        return $this;
    }
}
