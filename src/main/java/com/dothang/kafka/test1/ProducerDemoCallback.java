package com.dothang.kafka.test1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallback {

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
		
		// Producer properties		
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// Producer Record
		
		
		// send data
		for (int i = 111; i < 115; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello from java api " + Integer.toString(i));
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						logger.info(String.format("Received new metadata. \n Topic: %s \n Partition: %s \n Offset: %s \n Timestamp: %s \n",
									metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));	
					}
					else {
						logger.error("Error");
					}
				}
			});
		}
		
		
		producer.flush();
		producer.close();
	}

}
