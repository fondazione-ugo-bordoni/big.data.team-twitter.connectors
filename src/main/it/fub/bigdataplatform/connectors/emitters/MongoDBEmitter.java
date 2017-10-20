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

package it.fub.bigdataplatform.connectors.emitters;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.json.simple.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import it.fub.bigdataplatform.facades.MongoDBFacade;

public class MongoDBEmitter implements Emitter {
	
	private MongoDBFacade dbFacade;
	
	public MongoDBEmitter(){
		
		try {
			dbFacade = new MongoDBFacade("../etc/mongodb.properties");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void emit(JSONObject tweet) {
		
		Object o = com.mongodb.util.JSON.parse(tweet.toJSONString());
		BasicDBObject dbObj = (BasicDBObject) o;
		//conversione dei campi created_at da String a Date
		createdAtToDate(dbObj);
		dbFacade.store(dbObj);
	}


	private void createdAtToDate(BasicDBObject tweetToBeChecked){
		
		SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);	
		BasicDBObject user = (BasicDBObject) tweetToBeChecked.get("user");

		try {
			//sostituisco il created_at(String) del tweet con l'oggetto Date corrispondente
			Date createdAtAsDate = formatter.parse((String)tweetToBeChecked.get("created_at"));
			tweetToBeChecked.put("created_at", createdAtAsDate);		
			
			//sostituisco il created_at(String) dello user con l'oggetto Date corrispondente
			createdAtAsDate = formatter.parse((String)user.get("created_at"));
			user.put("created_at", createdAtAsDate);
			tweetToBeChecked.put("user", user);				

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		BasicDBObject retweeted_status = (BasicDBObject) tweetToBeChecked.get("retweeted_status");	
		if (retweeted_status!=null){
			createdAtToDate(retweeted_status);
			tweetToBeChecked.put("retweeted_status", retweeted_status);		
		}
	}
	
}
