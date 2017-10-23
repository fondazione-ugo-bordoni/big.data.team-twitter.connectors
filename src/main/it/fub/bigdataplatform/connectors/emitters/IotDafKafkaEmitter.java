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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.json.simple.JSONObject;

import it.fub.bigdataplatform.utils.Configuration;
import it.gov.daf.iotingestion.event.Event;

public class IotDafKafkaEmitter implements Emitter{

	private Properties props;
	private Properties kafkaProps;
	private KafkaProducer<byte[],byte[]> producer;

	public void KafkaEmitter(){
			//TODO CAPIRE PERCHE' NON POSSO INIZIALIZZARE NEL COSTRUTTORE LE PROPRIETA'
			// DELL'EMITTER. SEMBRA SOVRASCRIVERE LE PROPRIETA' DEL CONNETTORE (?!?)		
	}
	
	@Override
	public void emit(JSONObject jsonObject) {

		props = Configuration.loadProps("../etc/iot-daf-kafka.properties");
		
		kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("kafka.bootstrap.servers"));
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
	
		
		KafkaProducer<byte[],byte[]> producer = new KafkaProducer<byte[], byte[]>(kafkaProps); 		
			
		System.out.println("jsonObject");
		String topic =  props.getProperty("kafka.topic");
		IoTDafEvent event = new IoTDafEvent();

		event.wrapTweet(jsonObject);
	
		ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>(topic, event.id, event.payload); 
	
		producer.send(record);
		producer.close();
		
	}

	@Override
	public void finalize(){
	}
	
	private class IoTDafEvent{
	
		byte[] id;
		byte[] payload;
		
		public void wrapTweet(JSONObject jsonObject) {
			
			String eventId = "twitter-" + jsonObject.get("id");
			
			SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
			
			Date createdAtAsDate = new Date();
			
			try {
				createdAtAsDate = formatter.parse((String)jsonObject.get("created_at"));
			} catch (ParseException e) {
				System.err.println(e.getMessage());
			}
			
			//value = jsonObject.toJSONString().getBytes();
		
			java.lang.Long version = 1L; 
			java.lang.Long timestamp = createdAtAsDate.getTime();
			java.lang.CharSequence temporalGranularity=null; 
			java.lang.Double eventCertainty = 1D; 
			java.lang.Integer eventTypeId = 2; 
			java.lang.CharSequence eventSubtypeId = "tweet"; 
			java.lang.CharSequence eventAnnotation = null; 
			java.lang.CharSequence source = "twitter"; 
			java.lang.CharSequence location =""; 
			java.nio.ByteBuffer body = java.nio.ByteBuffer.wrap(jsonObject.toJSONString().getBytes());
			
			java.util.Map<java.lang.CharSequence,java.lang.CharSequence> attributes = new HashMap();
			JSONObject jsonUser = (JSONObject) jsonObject.get("user");
			attributes.put("user-id", jsonUser.get("id").toString());
			attributes.put("user-screen_name", jsonUser.get("screen_name").toString());
			
			Event event = new Event(version, eventId, timestamp, temporalGranularity, eventCertainty, eventTypeId, eventSubtypeId, eventAnnotation, source, location, body, attributes);
		
			id = eventId.getBytes();
			
			SpecificDatumWriter<Event> writer = new SpecificDatumWriter<Event>(Event.getClassSchema());
			
		    ByteArrayOutputStream stream = new ByteArrayOutputStream();
			BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null);
			try {
				writer.write(event, binaryEncoder);
				binaryEncoder.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			payload = stream.toByteArray();
					
		}	
	}
	
	public static void main(String[] args){
		IotDafKafkaEmitter emitter = new IotDafKafkaEmitter();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("id", "918120509929291776");
		jsonObject.put("text","RT @vitocrimi: Fiducia su legge elettorale vuol dire che non é una legge condivisa. É un atto eversivo e useremo ogni mezzo per impedirlo.");
		jsonObject.put("created_at","Mon Oct 13 16:39:35 CEST 2014");
		
		JSONObject jsonUserObject = new JSONObject(); 
		jsonUserObject.put("id", "232323922");
		jsonUserObject.put("screen_name", "testScreenName");
		jsonObject.put("user",jsonUserObject);
		
		System.out.println(jsonObject.toJSONString());
		
		emitter.emit(jsonObject);
	}
}