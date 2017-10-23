# FUB Big Data Platform: Twitter Connectors

This project contains a standalone Java application for collecting tweets from the Twitter streaming API.

You can configure the application to:

- store tweets into a Mongo DB instance;
- send tweets to a Kafka server;
- print tweets on the screen (useful for testing).

You can easily to implement other *emitters* to send collected tweets to you preferred storage or processing system.

You can specify the set of tweets to collect defining:

- a set of keywords to be monitored (max. 400);
- a set of account ids to be monitored (max. 5000).

This limitations are due to the rules of the Twitter streaming end-point. For more information see: 

<https://developer.twitter.com/en/docs/tweets/filter-realtime/overview> 

The application also support an alarm mechanism able to send emails when something goes wrong. For example, since Twitter streaming end-poit works in best-effort mode, when you define a too large set of tweets to monitor, the application can be configured to notify you when you are loosing more than a specified percentage of tweets.       

## Pre-requisites

To run the application you just need a *Java 1.6+* virtual machine. You can download the application from the `./download` directory of this repository.

To compile and run the application you also requires *Apache Ant*.

## How to configure the application
To configure the application you have to create the following files in the `$CONNECTOR_HOME/etc` directory:

- `twitter-connector.properties`. Starting from twitter-connector.properties.sample file already existing in the same directory, you have to properly set:

	- the application keys values (*twitter.consumerKey, twitter.consumerSecret, twitter.token, twitter.tokenSecret*). To do this you have to register yourself as a developer of twitter application and to create your own application (see ...).
	
	- the emitters you want to use (*twitter.emitters*).

	- the keywords to be tracked (*twitter.streaming.filter.track.keywords*).
	
	- (optionally) the twitter ids to be monitored (*twitter.streaming.filter.follow.ids*).
 
 And other minor parameters you find in the sample config file.
 
- On the basis of the emitter you have selected you have to properly configure one or more of the following files:

 - `mongodb.properties` to set your MongoDB parameters;
 - `kafka.properties` to set your Kafka server parameters.

- If you enable the email alarm mechanism, you can set messages to send in the file `alarm.properties`.

If you are developing the application be sure to copy your configuration files in the `./dist/etc` directory and to run the application in the `./dist/bin` directory.
 
## How to run the application

To start the application you have simply run the `./bin/run_twitter_connector.sh` script in the `$CONNECTOR_HOME`.


## How to rebuild the application

To rebuild the application launch the following command:

`ant -dist`

The *dist* directory will contain the compiled version of the application.