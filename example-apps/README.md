# RxRabbit Tutorial

This is a step by step guide that will help to get you started with the rxrabbit library and explore its capabilities.
It should not take more than 5 minutes to get up and running.
 
### Pre requisites
 - [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
 - [docker](https://docs.docker.com/)  (version 1.9 or later)
 - [docker-compose](https://docs.docker.com/compose/)  (version 1.6 or later)

### Step 1: Clone the repo and go to example-apps

Open a terminal, clone the repo and move in to the __example-apps__ folder.

    git clone git@github.com:meltwater/rxrabbit.git 
    cd rxrabbit/example-apps

The example-apps folder contains all the necessary resources to to spin up a local development environment. 

It contains two java main classes that can be used as a starting point for developing your own application:

[ExampleAppShovel](src/main/java/com/meltwater/rxrabbit/example/ExampleAppShovel.java) - Consumes messages from one queue and publishes them to another queue

[LoadGenerator](src/main/java/com/meltwater/rxrabbit/example/LoadGenerator.java) - Generates example messages and publishes them to an exchange.

It also contains a [docker-compose.yml](docker-compose.yml) file that will start up a rabbit broker and prime it with test queues and exchanges. 
We use [rabbit-puppy](https://github.com/meltwater/rabbit-puppy) to configure the rabbit broker.

### Step 2: Start the broker
Start up RabbitMQ.

    docker-compose up

After the broker has started you can navigate to the rabbit broker management ui [localhost:15672](http://localhost:15672/#/queues) in your browser (use __guest/guest__ as username and password).

Note that docker-compose will not only start up a broker but also configure it according to the contents of [rabbit_puppy/rabbitcfg.yaml](rabbit_puppy/rabbitcfg.yaml). So you will see some test queues and exchanges already configured for your convenience.

You can use __Ctrl+C__ to stop docker-compose - but let it run for now.

### Step 3: Run the LoadGenerator
In another terminal, run the [LoadGenerator](src/main/java/com/meltwater/rxrabbit/example/LoadGenerator.java) app from gradle like this.
 
    ../gradlew runLoadGenerator -Dpublish.message.count=5000

This will publish 5000 messages to the test-in queue and then quit. You can verify that the messages has been published by looking in the rabbit broker management ui.

### Step 4: Run the ExampleAppShovel    
The next step is to run the [ExampleAppShovel](src/main/java/com/meltwater/rxrabbit/example/ExampleAppShovel.java) like this:

    ../gradlew runShovel
        
This will move the messages from the test-in queue to test-out queue. Note that the above gradle task will never stop by itself. You have to type __Ctrl+C__ to halt it.

### Step 5: Test consumer failure recovery
If you haven't stopped the shovel you can test its auto-connect feature by letting it run, and then at the same time __restart the broker__, and see what happens.

- Stop the broker by doing Ctrl+C on the docker compose terminal (with the shovel running in another)
- Start the broker again ```docker-compose up```
- Watch the logs and/or the rabbit broker management ui (you can look in the connections tab, or on the test-in queue page) to see that the example apps automatically re-connects

### Step 6: Test publish re-try
You can also test the re-publish capabilities like this.

- Start the broker (if its not already running) ```docker-compose up```
- Start the load generator and configure it to publish a large amount of messages ```../gradlew runLoadGenerator -Dpublish.message.count=100000```
- When publishing has started, stop the broker by doing Ctrl+C on the docker compose terminal
- Then start the broker again ```docker-compose up```

You will see from the logs that the publisher re-tries failed publishes. In the end you might have more than 100000 messages in the broker. That is expected - the important thing is that every message is *delivered at least once*. 

If you change the code and remove the publisher confirms setting in the [LoadGenerator](src/main/java/com/meltwater/rxrabbit/example/LoadGenerator.java) you might get the opposite; that messages are *delivered at most once*.


### Step 7: Running everything at the same time
It is also possible to run both of the example apps in a single gradle command like this

    ../gradlew runLoadGenerator runShovel -Dpublish.message.count=10000 --parallel

*NOTE* if you want to adjust the log levels of the example apps edit [src/main/resources/logback.xml/rabbitcfg.yaml](src/main/resources/logback.xml)

This concludes the tutorial.
