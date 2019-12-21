package com.dothang.kafka.test1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		
		// Producer properties		
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// Producer Record
		
		
		// send data
		for (int i = 1; i < 10; i++) {
			String topic = "first_topic";
			String valueString = "hello world " + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, valueString);
			logger.info(String.format("Key: %s", key));
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
			}).get();
		}
		
		
		producer.flush();
		producer.close();
	}

}
