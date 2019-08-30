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

##### Simple example
```php
<?php

use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->addBroker('localhost:9092')
    ->build();

$message = KafkaMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test message payload')
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message);
```
##### Avro example
```php
<?php

use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->addBroker('kafka:9092')
    ->addSchemaRegistryUrl('schema-registry:8081')
    ->build();

$schemaName = 'testSchema';
$version = 1;
$message = KafkaMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": "some name"}') //this must be a json encoded string
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message, $schemaName, $version);
```

**NOTE:** To improve producer latency you can install the `pcntl` extension.  
The messaging-lib already has code in place, similarly described here:  
https://github.com/arnaud-lb/php-rdkafka#performance--low-latency-settings

### Consumer

#### Kafka High Level

```php
<?php

use \Jobcloud\Messaging\Consumer\ConsumerException;
use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;

$consumer = KafkaConsumerBuilder::create()
     ->addConfig(
        [
            'compression.codec' => 'lz4',
            'auto.offset.reset' => 'earliest',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->addBroker('kafka:9092')
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
     ->addConfig(
        [
            'compression.codec' => 'lz4',
            'auto.offset.reset' => 'earliest',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->addBroker('kafka:9092')
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

#### Avro Consumer
To create an avro consumer add the schema url and your schema name(s) and optional versions.

```php
<?php

use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Consumer\KafkaReaderSchema;

$schemaName = 'someSchema';
$version = 9;

$consumer = KafkaConsumerBuilder::create()
    ->addSchemaRegistryUrl('schema-registry:8081')
    ->addReaderSchema('topicName', new KafkaReaderSchema($schemaName, $version))
    //some more code here
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

$message = KafkaMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

$pool->produce($message);
```
