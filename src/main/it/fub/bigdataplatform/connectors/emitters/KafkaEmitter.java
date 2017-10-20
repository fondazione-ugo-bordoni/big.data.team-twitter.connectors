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

import java.util.Properties;

import org.json.simple.JSONObject;

import it.fub.bigdataplatform.utils.Configuration;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaEmitter implements Emitter{

	private Properties props;
	private Properties kafkaProps;

	public void KafkaEmitter(){
			//TODO CAPIRE PERCHE' NON POSSO INIZIALIZZARE NEL COSTRUTTORE LE PROPRIETA'
			// DELL'EMITTER. SEMBRA SOVRASCRIVERE LE PROPRIETA' DEL CONNETTORE (?!?)
	}
	
	@Override
	public void emit(JSONObject jsonObject) {

		props = Configuration.loadProps("../etc/kafka.properties");
		
		kafkaProps = new Properties();
		kafkaProps.put("zk.connect", props.getProperty("kafka.zk.connect"));
		kafkaProps.put("metadata.broker.list", props.getProperty("kafka.metadata.broker.list"));
		kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
		
		//props.put("partitioner.class", "example.producer.SimplePartitioner");
		//props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(kafkaProps);
		Producer<String, String> producer = new Producer<String, String>(config);
		String topic = props.getProperty("kafka.topic");
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, jsonObject.toJSONString());
		
		//System.out.println("Sending... " + jsonObject.toJSONString());
		
		producer.send(data);
		
		producer.close();

	}

	@Override
	public void finalize(){
		//producer.close();
	}

	
	public static void main(String[] args){
		
		KafkaEmitter emitter = new KafkaEmitter();
		
		JSONObject jsonObject = new JSONObject();
		
		jsonObject.put("key", "value");
		
		emitter.emit(jsonObject);

	}

}
