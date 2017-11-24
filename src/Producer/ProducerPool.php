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
     * @param string|NULL $key
     */
    public function produce(string $message, string $topic)
    {
        foreach ($this->producers as $producer) {
            $producer->produce($message, $topic);
        }
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
