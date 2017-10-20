/**
 * Copyright 2017 Fondazione Ugo Bordoni
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.fub.bigdataplatform.connectors.twitter.streaming;

import it.fub.bigdataplatform.connectors.emitters.Emitter;
import it.fub.bigdataplatform.connectors.emitters.EmitterFactory;
import it.fub.bigdataplatform.connectors.twitter.utils.StatusConverter;
import it.fub.bigdataplatform.utils.Configuration;
import it.fub.bigdataplatform.utils.EmailSender;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONObject;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public abstract class StreamingListener implements StatusListener {
	
	protected TwitterStream twitterStream; 
	protected Properties props;
	
	protected ConfigurationBuilder cb;
	protected String consumerKey;
	protected String consumerSecret;
	protected String token;
	protected String tokenSecret;
	
	
	protected boolean includeEntities;
//	protected boolean simulateDB;
	protected boolean debugEnabled;
	protected String[] emitterNames;

	private int listenerHealthVerifierPeriodicity;
	private int maxLossPercentage;

	private long deliveredTweets;
	private long undeliveredTweets;

	protected StatusConverter statusConverter;
	
	protected Emitter[] emitters;
	
	private Logger logger;

	protected StreamingListener(){
		
		PropertyConfigurator.configure("../etc/log4j.properties");
		logger =  Logger.getLogger(this.getClass().getName());
		
		readProperties();
		verifyProperties();
		
		deliveredTweets=0;
		undeliveredTweets=0;

		new ListenerHealthVerifier(this, listenerHealthVerifierPeriodicity, maxLossPercentage).start();
		
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(debugEnabled);
		cb.setOAuthConsumerKey(consumerKey);
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(token);
		cb.setOAuthAccessTokenSecret(tokenSecret);
		cb.setIncludeEntitiesEnabled(includeEntities);

		emitters = EmitterFactory.getEmitters(emitterNames);
		
		statusConverter = new StatusConverter(includeEntities);
		
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(this);

	}

	private void verifyProperties() {
		if (consumerKey==null) {
			logger.fatal("Error: twitter.consumerKey is not specified. Check your system.property file.");
			System.exit(-1);
		}

		if (consumerSecret==null) {
			logger.fatal("Error: twitter.consumerSecret is not specified. Check your system.property file.");
			System.exit(-1);
		}

		if (token==null) {
			logger.fatal("Error: twitter.token is not specified. Check your system.property file.");
			System.exit(-1);
		}

		if (tokenSecret==null) {
			logger.fatal("Error: twitter.tokenSecret is not specified. Check your system.property file.");
			System.exit(-1);
		}	
	}

	private void readProperties() {
		
		props = Configuration.loadProps("../etc/twitter-connector.properties");

		consumerKey = props.getProperty("twitter.consumerKey");
		consumerSecret = props.getProperty("twitter.consumerSecret");
		token = props.getProperty("twitter.token");
		tokenSecret = props.getProperty("twitter.tokenSecret");
		
		listenerHealthVerifierPeriodicity = Integer.parseInt(props.getProperty("twitter.streaming.filter.verification.period","5"));
		maxLossPercentage = Integer.parseInt(props.getProperty("twitter.streaming.filter.verification.maxLossPercentage","20"));
		
		includeEntities = Boolean.parseBoolean(props.getProperty("twitter.includeEntities","true"));
		debugEnabled = Boolean.parseBoolean(props.getProperty("twitter.debugEnabled","false"));
		
		emitterNames = getEmitterNames(props.getProperty("twitter.emitters"));
		
	}

	
	private String[] getEmitterNames(String property) {

		String[] emitters = property.split(",");
		
		for (int i = 0; i < emitters.length; i++){
			emitters[i] = emitters[i].trim();
		}
		
		return emitters;
	}

	@Override
	public void onStatus(Status status) {
		JSONObject tweet = statusConverter.toJSON(status);
		for (int i=0;i<emitters.length;i++){
			emitters[i].emit(tweet);
		}
	}

	/**
	 * Metodo invocato in presenza di un "Status deletion notices (delete)" message inviato da Twitter.
	 * 
	 * These messages indicate that a given Tweet has been deleted. 
	 * Client code must honor these messages by clearing the referenced Tweet from memory 
	 * and any storage or archive, even in the rare case where a deletion message arrives 
	 * earlier in the stream that the Tweet it references.
	 * 
	 * @see https://dev.twitter.com/docs/streaming-apis/messages#Status_deletion_notices_delete
	 */
	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		logger.info("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
	}

	/**
	 * Metodo invocato in presenza di un "Location deletion notices (scrub_geo)" message inviato da Twitter.
	 * 
	 * These messages indicate that geolocated data must be stripped from a range of Tweets.
	 * Clients must honor these messages by deleting geocoded data from Tweets which fall before 
	 * the given status ID and belong to the specified user. These messages may also arrive before 
	 * a Tweet which falls into the specified range, although this is rare.
	 * 
	 * @see https://dev.twitter.com/docs/streaming-apis/messages#Location_deletion_notices_scrub_geo
	 */
	public void onScrubGeo(long userId, long upToStatusId) {
		logger.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
	}

	/**
	 * Metodo invocato in caso di eccezione sollevata durante lo streaming.
	 */
	public void onException(Exception ex) {
		logger.error("Exception sent by Twitter: " + ex.getMessage());
		sendEmail(ex.getMessage());
	}

	/**
	 * Metodo invocato in presenza di un "Stall warnings (warning)" message inviato da Twitter.
	 * 
	 * When connected to a stream using the stall_warnings parameter, you may receive status 
	 * notices indicating the current health of the connection. See the stall_warnings documentation 
	 * for more information.
	 * 
	 * @see https://dev.twitter.com/docs/streaming-apis/messages#Limit_notices_limit
	 * 
	 */
	public void onStallWarning(StallWarning warning) {
		logger.warn("Got stall warning:" + warning);
		sendEmail(warning.getMessage());
	}
	
	/**
	 * Metodo invocato in presenza di un "limit notice" message inviato da Twitter.
	 * 
	 * "These messages indicate that a filtered stream has matched more Tweets than 
	 * its current rate limit allows to be delivered. Limit notices contain a total 
	 * count of the number of undelivered Tweets since the connection was opened, 
	 * making them useful for tracking counts of track terms, for example. Note that 
	 * the counts do not specify which filter predicates undelivered messages matched."
	 * 
	 * @see https://dev.twitter.com/docs/streaming-apis/messages#Limit_notices_limit
	 */
	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		logger.warn("Total number of undelivered tweets = " + numberOfLimitedStatuses);
		undeliveredTweets =numberOfLimitedStatuses;
	}

	public long getTotalNumberOfLostTweets() {
		return this.undeliveredTweets;
	}

	public long getTotalNumberOfStoredTweets() {
		return deliveredTweets;
	}

	protected void sendEmail(String message){

		String ip = "Error reading local IP address";

		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.error(e.getMessage());
		}

		String msgToBeSent;

		msgToBeSent = message +
				"\n *** Message sent by " + ip;

		new EmailSender().send(msgToBeSent);

		logger.info("Sending email with message: " + msgToBeSent);

	}

	private class ListenerHealthVerifier extends Thread{

		private StreamingListener listener;
		private int periodicity;
		private int maxPercentageOfLostTweets;

		private long lastValueOfNumOfLostTweet;
		private long currentValueOfNumOfLostTweet;

		private long lastValueOfNumOfStoredTweet;
		private long currentValueOfNumOfStoredTweet;

		private Logger logger;

		/** 
		 * @param listener
		 * @param periodicity in minutes
		 * @param maxPercentageOfLostTweets
		 */
		ListenerHealthVerifier(StreamingListener listener, int periodicity, int maxPercentageOfLostTweets){
			this.listener = listener;
			PropertyConfigurator.configure("../etc/log4j.properties");
			logger =  Logger.getLogger(this.getClass().getName());			
			lastValueOfNumOfLostTweet  = listener.getTotalNumberOfLostTweets();
			lastValueOfNumOfStoredTweet = listener.getTotalNumberOfStoredTweets();
			this.maxPercentageOfLostTweets = maxPercentageOfLostTweets;
			this.periodicity=periodicity;
		}

		@Override
		public void run() {

			while(true){

				try {
					sleep(periodicity * 60000); // convert minutes in milliseconds:  x mins => xmins * 60s * 1000 ms
				} catch (InterruptedException e) {
					logger.error(e.getCause());
				}

				currentValueOfNumOfLostTweet = listener.getTotalNumberOfLostTweets();
				currentValueOfNumOfStoredTweet = listener.getTotalNumberOfStoredTweets();

				verifyThreshold();

				lastValueOfNumOfLostTweet = currentValueOfNumOfLostTweet;
				lastValueOfNumOfStoredTweet = currentValueOfNumOfStoredTweet;
			}	
		}

		private void verifyThreshold() {

			long lostTweets = currentValueOfNumOfLostTweet - lastValueOfNumOfLostTweet;
			long storedTweets = currentValueOfNumOfStoredTweet - lastValueOfNumOfStoredTweet;

			if (lostTweets>0){
				if (storedTweets>0){
					double percentage = (lostTweets * 100) / storedTweets;
					logger.info(percentage + "% of lost tweets..."); 
					if (percentage>maxPercentageOfLostTweets){
						listener.sendEmail("Warning: in the last " + periodicity + " minutes the connector "+
								"has lost the " + percentage + "% of tweets.\n "+
								"Please verify the track is not too much noisy.");
					}
				} else {
					listener.sendEmail("Warning: check the connector. It seems the connector has not stored any tweets " +
							"in the last " + periodicity + " minutes.");
				}
			} else {
				logger.info("0% of lost tweets...");
			}
		}
	}
}