# RxRabbit

RxRabbit is a [RabbitMQ](https://www.rabbitmq.com/) java 8 client library that extends and enhances the [rabbitmq-java-client](https://www.rabbitmq.com/java-client.html) by providing a resilient, auto-connecting [ReactiveX](http://reactivex.io/) styled API.

[ ![Download](https://api.bintray.com/packages/meltwater/opensource/rxrabbit/images/download.svg) ](https://bintray.com/meltwater/opensource/rxrabbit/_latestVersion)

## Highlights

- Simple, reactive API based on [RxJava](https://github.com/ReactiveX/RxJava) that fits the RabbitMQ model.
- Automatic [error handling](#error-handling-&-recovery) and recovery that 'just works', both for publishing and consuming.

## Getting started

Have a look at the [RxRabbit Tutorial](example-apps) to get a feel for how to use the API in a real application.

You can also look at the [integration tests](rxrabbit/src/test/groovy/com/meltwater/rxrabbit/RxRabbitTests.java) and the [ExampleCode class](rxrabbit/src/test/java/com/meltwater/rxrabbit/example/ExampleCode.java).

The javadoc of the core API classes and interfaces is also a good source of reference.


## Download dependencies

*Gradle:*

```groovy    
    compile 'com.meltwater:rxrabbit:$RXRABBIT_VERSION'   
```
       
*Maven:*

```xml  
    <dependency>
        <groupId>com.meltwater</groupId>
        <artifactId>rxrabbit</artifactId>
        <version>$RXRABBIT_VERSION</version>
        <type>jar</type>
    </dependency>
```

**NOTE** the rxrabbit binaries is currently hosted on [jcenter](https://bintray.com/bintray/jcenter).

## Design Philosophy

### Opinionated API
The API has strong opinions on how RabbitMQ should be used. It hides a lot of functionality from the Channel interface and also introduces a concept of **channel types** that are made for a specific purpose (such as publish, consume or admin operations). 
This means of course that there are several things that you can't do with this api, but keep in mind that it is a conscious decision made by the API developers.

Main supported use cases by rxrabbit:

- Continuously 'infinite' consume (with manual acknowledgment) from an already existing queue on the rabbit broker.
- Continuously 'infinite' consume (with manual acknowledgment) from a server created, temporary, exclusive queue bound to an existing exchange on the rabbit broker.
- Publish messages to an exchange with (or without) publisher confirmation but with mandatory=false and immediate=false.
- Perform basic 'admin' operations (declare, remove and purge queues, exchanges and bindings)

### Error handling and recovery
The [official rabbitmq Java client](https://github.com/rabbitmq/rabbitmq-java-client) provides some basic but non-complete error handling and recovery mechanisms. 
A number of wrapper libraries already exists today which, with varying success, automatically handles connection recovery in more error scenarios than the official java client.
The best ones we have found so far were [Spring-amqp](http://projects.spring.io/spring-amqp/) which we discovered suffered from excessive and erroneous channel handling, and
[lyra](https://github.com/jhalterman/lyra) which, in our view, implements and overly complex error handling logic that still has issues with recovering from some extreme error cases, such as hard broker restarts.

RxRabbit instead uses a very basic but effective approach when it comes to error handling and recovery:

**No matter the error, the code assumes that the connection is broken then  attempts to re-connect to the broker with exponential back-off**

The goal we have is that consumers and producers should not have to care about the underlying rabbit channels and connections. Re-connects should remain hidden from the API user unless specifically asked for (currently achieved by adding listeners).

## Building locally

**Pre-requisites**
 - [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
 - [docker](https://docs.docker.com/)  (version 1.9 or later)
 - [docker-compose](https://docs.docker.com/compose/)  (version 1.6 or later)

Build (including running the tests) by running 

    ./gradlew clean build

Also note that it is *currently NOT supported to run the test on OSX* using *docker machine*, you need to be able to connect to docker containers using localhost:<port>.

## How to contribute

We happily accept contributions in the form of [Github PRs](https://help.github.com/articles/about-pull-requests/) 
or in the form of bug reports, comments/suggestions or usage questions by creating a [github issue](https://github.com/meltwater/rxrabbit/issues).

## License
The MIT License (MIT)

Copyright (c) 2016 Meltwater Inc. http://underthehood.meltwater.com/
