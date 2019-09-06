# messaging-lib

[![CircleCI](https://circleci.com/gh/jobcloud/messaging-lib.svg?style=svg)](https://circleci.com/gh/jobcloud/messaging-lib)
 [![Maintainability](https://api.codeclimate.com/v1/badges/7b01ab13705d4be203a1/maintainability)](https://codeclimate.com/github/jobcloud/messaging-lib/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/7b01ab13705d4be203a1/test_coverage)](https://codeclimate.com/github/jobcloud/messaging-lib/test_coverage) [![Latest Stable Version](https://poser.pugx.org/jobcloud/messaging-lib/v/stable)](https://packagist.org/packages/jobcloud/messaging-lib) [![Latest Unstable Version](https://poser.pugx.org/jobcloud/messaging-lib/v/unstable)](https://packagist.org/packages/jobcloud/messaging-lib) 
## Description
Generic php messaging library
Supports:
- Kafka

This is a convenience wrapper for [arnaud-lb/php-rdkafka](https://github.com/arnaud-lb/php-rdkafka)  
Avro support relies on [flix-tech/avro-serde-php](https://github.com/flix-tech/avro-serde-php)  
To read more about the functions used in this lib, check out the [documentation](https://arnaud.le-blanc.net/php-rdkafka/phpdoc/book.rdkafka.html) of the extension.


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

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage;
use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->addBroker('localhost:9092')
    ->build();

$message = KafkaProducerMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test message payload')
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message);
```
##### Avro Producer
To create an avro prodcuer add the avro normalizer.  

```php
<?php

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder;
use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;
use \Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformer;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;

$normalizer = new AvroEncoder(
    new AvroTransformer(
        new CachedRegistry(
            new BlockingRegistry(
                new PromisingRegistry(
                    new Client(['base_uri' => 'jobcloud-kafka-schema-registry:9081'])
                )
            ),
            new AvroObjectCacheAdapter()
        )
    ),
    [
        'test' => new KafkaAvroSchema('test' /* , $schemaVersion */)
    ]
);

$producer = KafkaProducerBuilder::create()
    ->addBroker('kafka:9092')
    ->setNormalizer($normalizer)
    ->build();

$schemaName = 'testSchema';
$version = 1;
$message = KafkaProducerMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": "some name"}') //this must be a json encoded string
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message);
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
To create an avro consumer add the avro denormalizer.  
If `AvroDenormalizer` is instantiated with an empty array (`$schemaMapping`),  
the latest version of the schema that was used for encoding will be used.

```php
<?php

use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Message\Decoder\AvroDecoder;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformer;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;

$denormalizer = new AvroDecoder(
    new AvroTransformer(
        new CachedRegistry(
            new BlockingRegistry(
                new PromisingRegistry(
                    new Client(['base_uri' => 'jobcloud-kafka-schema-registry:9081'])
                )
            ),
            new AvroObjectCacheAdapter()
        )
    ),
    [
        'test' => new KafkaAvroSchema('test', 1) //this entry is optional, on empty array, latest schema is used
    ]
);

$schemaName = 'someSchema';
$version = 9;

$consumer = KafkaConsumerBuilder::create()
     ->addConfig(
        [
            'compression.codec' => 'lz4',
            'auto.offset.reset' => 'earliest',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->setDenormalizer($denormalizer)
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
