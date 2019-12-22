package com.dothang.kafka.test1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoSeek {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
		long offsetToReadFrom = 15L;
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessagesReadSoFar = 0;
		while(keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record: records) {
				logger.info(String.format("Key: %s - Value: %s ", record.key(), record.value()));
				logger.info(String.format("Partition: %s - Offset %s\n", record.partition(), record.offset()));
				
				if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
					keepOnReading = false;
					break;
				}
				numberOfMessagesReadSoFar += 1;
			}
		}
		logger.info("Exit application");	
	}

}
