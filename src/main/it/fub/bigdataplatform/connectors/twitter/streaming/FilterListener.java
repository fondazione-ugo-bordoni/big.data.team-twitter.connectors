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

import java.net.UnknownHostException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Logger;

import twitter4j.FilterQuery;

import com.mongodb.MongoException;

/**
 * Implementa le azioni da eseguire all'arrivo di un tweet.
 *
 * @author Marco Bianchi (mbianchi@fub.it)
 */

public class FilterListener extends StreamingListener {

	private String[] keywords;
	private String[] languages;
	private long[] followIDs;

	private Logger logger;

	/**
	 * Costruttore.
	 * Configura il connettore di streaming e il collegamento con il db.
	 * 
	 * @throws UnknownHostException
	 * @throws MongoException
	 */

	public FilterListener() throws UnknownHostException, MongoException{

		super();

		//PropertyConfigurator.configure("../etc/log4j.properties");
		logger = Logger.getLogger(this.getClass().getName());

		readProperties();
		verifyProperties();

		listen(this.keywords, this.followIDs, this.languages);

	}

	private void extractKeywords(String track) {
	
		if (track!=null){
			StringTokenizer tokenizer = new StringTokenizer(track, ",");
			Vector<String> tracksVector = new Vector<String>();

			while (tokenizer.hasMoreTokens()){
				tracksVector.add(tokenizer.nextToken());
			}

			keywords = new String[tracksVector.size()];
			this.keywords = tracksVector.toArray(keywords);

			for (int i=0; i<this.keywords.length; i++){
				logger.info(this.keywords[i]);
			}
		}
	}

	void extractFollowIDs(String followIdsString) {

		if (followIdsString!=null){

			String[] follows = followIdsString.split(",");
			followIDs = new long[follows.length];
			for (int i = 0; i < follows.length; i++){
				followIDs[i] = Long.parseLong(follows[i]);
				logger.info(followIDs[i]);
			}
		}
	}
	
	private void extractLanguages(String langs) {
		if (langs!=null){
			StringTokenizer tokenizer = new StringTokenizer(langs, ",");
			Vector<String> languagesVector = new Vector<String>();

			while (tokenizer.hasMoreTokens()){
				languagesVector.add(tokenizer.nextToken());
			}

			languages = new String[languagesVector.size()];
			this.languages = languagesVector.toArray(languages);

			for (int i=0; i<this.languages.length; i++){
				logger.info(this.languages[i]);
			}
		}
		
	}

	private void verifyProperties() {

		if (keywords==null && followIDs==null) {
			logger.fatal("Error: Either twitter.streaming.filter.track.keywords or twitter.streaming.filter.follow.ids MUST be specified. Check your twitter.property file.");
			System.exit(-1);
		}
	}

	private void readProperties() throws UnknownHostException {

		String track = props.getProperty("twitter.streaming.filter.track.keywords");
		extractKeywords(track);

		String followIds = props.getProperty("twitter.streaming.filter.follow.ids");
		extractFollowIDs(followIds);
		
		String langs = props.getProperty("twitter.streaming.filter.languages");
		extractLanguages(langs);

	}

	/**
	 * Avvia l'ascolto su twitter delle parole chiave e degli userID passati per parametri.
	 * @param track
	 * @param follow
	 */

	private void listen(String[] track, long[] follow){
		listen(track,follow,null);
	}
	

	/**
	 * Avvia l'ascolto su twitter delle parole chiave e degli userID passati per parametri.
	 * Seleziona solo i tweet delle lingue indicate in input
	 * @param track
	 * @param follow
	 * @param languages
	 */
	private void listen(String[] track, long[] follow, String[] languages){

		FilterQuery fq;
		if (follow==null){
			fq = new FilterQuery();
		} else {
			fq = new FilterQuery(follow);
		}

		FilterQuery queries = fq.track(track);
		
		if(languages != null){
			fq.language(languages);
		}

		logger.info("Starting monitoring: " + queries.toString());
		twitterStream.filter(fq);	
	}

	
}