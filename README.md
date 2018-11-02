# messaging-lib

SemaphoreCI master: [![Build Status](https://semaphoreci.com/api/v1/nick-zh/messaging-lib-77823800-8a15-4a87-aaa6-531ca5e5cda8/branches/master/badge.svg)](https://semaphoreci.com/nick-zh/messaging-lib-77823800-8a15-4a87-aaa6-531ca5e5cda8)

## Description
Generic php messaging library
Supports:
- Kafka

## Requirements
- php: ^7.1
- ext-rdkafka: ^3.0.5

## Installation
```composer require jobcloud/messaging-lib "~1.0"```

## Usage

### Producer

#### Kafka

```php
<?php

use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->addBroker('10.0.2.2')
    ->build();

$producer->produce('hello world', 'testTopic');
```

### Consumer

#### Kafka

```php
<?php

use \Jobcloud\Messaging\Consumer\ConsumerException;
use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use \Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;

$topic = new TopicSubscription('testTopic');

$consumer = KafkaConsumerBuilder::create()
    ->addBroker('10.0.2.2')
    ->setConsumerGroup('testGroup')
    ->setTimeout(120 * 10000)
    ->addSubscription($topic)
    ->build();

while (true) {
    try {
        $message = $consumer->consume();
    } catch (ConsumerException $e) {
        // Failed
    } 
}
```

### ProducerPool

```php
<?php

use \Jobcloud\Messaging\Producer\ProducerPool;
use \Jobcloud\Messaging\Producer\ProducerInterface;

/** @var ProducerInterface $someKafkaProducer */
/** @var ProducerInterface $someRabbitMQProducer */

$pool = new ProducerPool();
$pool
    ->addProducer($someKafkaProducer)
    ->addProducer($someRabbitMQProducer)
;

$pool->produce('hello world', 'topicTest');
```