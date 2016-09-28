# RxRabbit: Java example applications

This module contains two main classes that can useful to look at as a starting point for other developers.

[ExampleApp] (src/main/java/com/meltwater/rxrabbit/java7/ExampleApp.java) - shows consume and publish

[LoadGenerator] (src/main/java/com/meltwater/rxrabbit/java7/LoadGenerator.java) - only for publishing

It is also interesting to look at the [properties file] (src/main/resources/default.properties) for the example application as it shows typical property
file contents for an application using rx-rabbit-meltwater.

To run the example apps, first set up the environment using the __docker-compose__ file in the root of this repo.
It will start up RabbitMQ and also use [rabbit-puppy] (https://github.com/meltwater/rabbit-puppy) to set up queues and exchanges.

NOTE: for links to work you need to ONLY look at the readme file, not the file tree view.