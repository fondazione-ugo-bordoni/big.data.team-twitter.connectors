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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class EmitterFactory {

	static private Logger logger;
	
	{	
		PropertyConfigurator.configure("../etc/log4j.properties");
		logger =  Logger.getLogger(this.getClass().getName());
	}

	public static Emitter getEmitter(String emitterName) {
			
		if (emitterName.equalsIgnoreCase("mongodb")){
			return new MongoDBEmitter();
		} else if (emitterName.equalsIgnoreCase("console")){
			return new ConsoleEmitter();
		} else if (emitterName.equalsIgnoreCase("kafka")){
			return new KafkaEmitter();
		} else if (emitterName.equalsIgnoreCase("daf-iot-kafka")){
			return new IotDafKafkaEmitter();
		}
		else {
			logger.fatal("Emitter not correctly specified. Please check the twitter.properties file.");
			System.exit(-1);
		}
		return null;
	}

	public static Emitter[] getEmitters(String[] emitterNames) {
		
		Emitter[] result = new Emitter[emitterNames.length];
		
		for (int i=0; i<result.length; i++){
			result[i] = getEmitter(emitterNames[i]);
		}
	
		return result;
	
	}
	
}