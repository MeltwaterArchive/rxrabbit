#rx-rabbit Release Notes
## next
### Changes
- No changes

## 0.13.0 - 2016-08-22
### Changes
- Adds metric for and logs id of re-delivered messages
- Can override statsd namespace with STATSD_NAMESPACE setting
- PublishEventListener#afterIntermediateFail logs ms instead of seconds

## 0.12.0 - 2016-07-07
### Changes
- Fixed SSL support (AMQPS)

## 0.11.3 - 2016-06-30
### Changes
- HOTFIX: Improvements to BulkRabbitPublisher. Should fix the test timeouts.

## 0.11.2 - 2016-06-21
### Changes
- SEARCH-5585: Make sure compression header is removed from bulk messages when consuming

## 0.11.1 - 2016-06-17
### Changes
- Do not return a new client properties map on every call to getClient_properties

## 0.11.0 - 2016-06-13
### Changes
- SEARCH-5586 Nacks and logs instead of failing when encountering faulty batches
- SEARCH-5506 Now detects queue-recreations and correctly re-connects  
- SEARCH-5506 Now handles broker failover by connecting to the next broker in the configured address list 

## 0.10.2 - 2016-03-26
### Changes
- Uses rxjava 1.1.1
- Updates of some other dependecies including docker-compose-java

## 0.10.1 - 2016-02-22
### Changes
- Moved repo to GitHub

## 0.10.0 - 2016-02-17
### Changes
- Adds TakeAndAckTransformer and uses it in some e2e tests. 
- Adds more logging in tests.

## v0.9.0 - 2016-02-04
### Changes
- Filters away rxjava parts of stack traces

### Fixes
- Unsubscribe unackedMessagesWorker when closing the channel
- Fixed concurrency bug in DefaultChannelFactory

## v0.8.0 - 2016-01-21
### Changes
- Improved log message when consuming from a non-existing queue

## v0.7.0 - 2016-01-08
### Changes
- Switched from using the BasicProperties interface to using the AMQP.BasicProperties implementation for message properties in RxRabbit Messages

## v0.6.0 - 2015-12-07
### Changes
- SEARCH-5107 Adds feature to consume from an exchange/routingKey pairLogging both before and after a rabbit connection is created
- Adds Readme.md and some tested example code snippets.
- More options for the LoadGenerator

## v0.5.1 - 2015-12-01
### Changes
- Removes the consumeWithMetrics method. It was confusing and dangerous.

### Compatibility Notes
- Can be used in java7 projects. 

## v0.5.0 - 2015-11-30
### Changes
- Refactors bulk messages to only include payload data. All messages in a given bulk need to have the same BasicProperties.
- Adds version to message headerRenames onDone and onFail to ack and rejectMoves all statsD set up code into the Meltwater specific factories in rx-rabbit-meltwater
- Timestamp and messageId are no longer set in the PublishQuiddity function.Single
- ChannelConsumer logs on warn level when messages are unacked for > 5 minutes

### Compatibility Notes
- Can be used in java7 projects. 

## v0.4.0 - 2015-11-24
### Changes
- Can read and send bulks (as tar.gz files).

### Compatibility Notes
- Can be used in java7 projects. 
- WARNING: Do not use Bulk API as it breaks in 0.5.0

## v0.3.0 - 2015-11-13
### Changes
- Can be used in java 7. 
- Adds retrolambda gradle plugin and replaces all non lambda jdk8 features. 
- Made RoundRobinPublisher thread safe.
- New publishing and quiddity converting apis
- Removed some unneccessary test depenedencies
- Adds more javadocRemoves subscribeOn and onBackpressureBuffer in ExampleApp

### Compatibility Notes
- Can be used in java7 projects.

## v0.2.1 - 2015-11-05
### Changes
- Moved and renamed test classes

### Fixes
- Fixed a bug in consume metric names

## v0.2.0 - 2015-11-05
### Changes
- Exposes new settings structure

## v0.1.0 - 2015-11-04
### Changes
- First version of rx-rabbit

