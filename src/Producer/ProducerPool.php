<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

final class ProducerPool implements ProducerInterface
{

    /**
     * @var ProducerInterface[]
     */
    private $producers = [];

    public function produce(string $message)
    {
        foreach ($this->producers as $producer) {
            $producer->produce($message);
        }
    }

    public function addProducer(ProducerInterface $producer): self
    {
        $this->producers[] = $producer;

        return $this;
    }
}
