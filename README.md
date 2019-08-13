# messaging-lib

[![CircleCI](https://circleci.com/gh/jobcloud/messaging-lib.svg?style=svg)](https://circleci.com/gh/jobcloud/messaging-lib)
 [![Maintainability](https://api.codeclimate.com/v1/badges/7b01ab13705d4be203a1/maintainability)](https://codeclimate.com/github/jobcloud/messaging-lib/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/7b01ab13705d4be203a1/test_coverage)](https://codeclimate.com/github/jobcloud/messaging-lib/test_coverage) [![Latest Stable Version](https://poser.pugx.org/jobcloud/messaging-lib/v/stable)](https://packagist.org/packages/jobcloud/messaging-lib) [![Latest Unstable Version](https://poser.pugx.org/jobcloud/messaging-lib/v/unstable)](https://packagist.org/packages/jobcloud/messaging-lib) 
## Description
Generic php messaging library
Supports:
- Kafka

This is a convenience wrapper for https://github.com/arnaud-lb/php-rdkafka  
To read more about the functions used in this lib, check out the documentation  
of the extension: https://arnaud.le-blanc.net/php-rdkafka/phpdoc/book.rdkafka.html


## Requirements
- php: ^7.1
- ext-rdkafka: ^3.1.2

## Installation
```composer require jobcloud/messaging-lib "~4.0"```

## Usage

### Producer

#### Kafka

```php
<?php

use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->addBroker('localhost:9095')
    ->build();

$producer->produce('hello world', 'testTopic');
```

### Consumer

#### Kafka High Level

```php
<?php

use \Jobcloud\Messaging\Consumer\ConsumerException;
use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;

$consumer = KafkaConsumerBuilder::create()
    ->addBroker('localhost:9095')
    ->setConsumerGroup('testGroup')
    ->setTimeout(120 * 10000)
    ->addSubscription('test-topic')
    ->build();

$consumer->subscribe();

while (true) {
    try {
        $message = $consumer->consume();
        // your business logic
        $consumer->commit($message);
    } catch (ConsumerException $e) {
        // Failed
    } 
}
```

#### Kafka Low Level

```php
<?php

use \Jobcloud\Messaging\Consumer\ConsumerException;
use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;

$consumer = KafkaConsumerBuilder::create()
    ->addBroker('localhost:9095')
    ->setConsumerGroup('testGroup')
    ->setTimeout(120 * 10000)
    ->addSubscription('test-topic')
    ->setConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL)
    ->build();

$consumer->subscribe();

while (true) {
    try {
        $message = $consumer->consume();
        // your business logic
        $consumer->commit($message);
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
