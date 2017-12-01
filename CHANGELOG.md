#RxRabbit Release Notes
## 1.3.0 - 2017-12-01
### Changes

- Adds all Maven Central required information to the pom file
- Using kotlin-compose v 1.2.1 that is also hosted on maven central
- Removes a flaky integration test (can_handle_multiple_consumers)
- Updating gradle to version 4.3.1
- Updating external dependencies such as guava, amqp-client and rxjava

## 1.2.0 - 2017-07-27
### Changes
- Fix threads leaking on retrying consumer on non existing queue

## 1.1.0 - 2017-01-19
### Changes
- Uses amqp-client 4.0.1
- Improves the long lived un acked messages log message.

## 1.0.1 - 2016-10-22
### Changes
- Upgrades rxjava, slf4j and guava dependencies.

## 1.0.0 - 2016-10-13
### Changes
- First open sourced public release.
