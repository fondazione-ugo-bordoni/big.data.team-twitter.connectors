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

package it.fub.bigdataplatform.facades;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

import it.fub.bigdataplatform.utils.Configuration;

public class MongoDBFacade {
	
	private Mongo mongo;
	private DB db;
	private DBCollection collection;
	
	private Properties props;
	private String host;
	private String database;
	private int bufferSize;
	private String collectionName;
	private List<DBObject> dbObjectBulk;
	
	private Logger logger;
	
	public MongoDBFacade(String propertyFilePath) throws UnknownHostException, MongoException{
		
		PropertyConfigurator.configure("../etc/log4j.properties");
		logger = Logger.getLogger(this.getClass().getName());

		props = Configuration.loadProps(propertyFilePath);
				
		host = props.getProperty("mongodb.ip");
		database = props.getProperty("mongodb.db");

		bufferSize = Integer.parseInt(props.getProperty("mongodb.buffersize"));
		collectionName = props.getProperty("mongodb.collection.name");
		
		mongo = new Mongo(host, 27017);
		db = mongo.getDB(database);
		collection = db.getCollection(collectionName);

		dbObjectBulk = new LinkedList<DBObject>();
	}

	public void store(BasicDBObject dbObj) {
		
		dbObjectBulk.add(dbObj);
		
		if (dbObjectBulk.size()>=bufferSize){
			logger.debug("Inserting " + dbObjectBulk.size() + ".");
			WriteResult result = collection.insert(dbObjectBulk);
			dbObjectBulk = new LinkedList<DBObject>();
		}
			
	}

	
}
