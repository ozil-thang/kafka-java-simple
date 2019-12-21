package com.dothang.kafka.test1;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoThread {
	private Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new ConsumerDemoThread().run();
	}
	
	public void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-seventh-application";
		String topic = "first_topic";
		
		CountDownLatch latch = new CountDownLatch(1);
		
		logger.info("Createing the consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);
		
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("Application got interrupt");
		}
		finally {
			logger.info("Application is closing");
		}
	}
	
	public class ConsumerRunnable implements Runnable{
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
		
		public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
			this.latch = latch;
			
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			consumer = new KafkaConsumer<String, String>(properties);
			consumer.subscribe(Collections.singleton(topic));
		}
		
		public void run() {
			// TODO Auto-generated method stub
			try {
				while(true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					
					for(ConsumerRecord<String, String> record: records) {
						logger.info(String.format("Key: %s - Value: %s ", record.key(), record.value()));
						logger.info(String.format("Partition: %s - Offset %s\n", record.partition(), record.offset()));
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal");
			}
			finally {
				consumer.close();
				latch.countDown();
			}
		}
		
		public void shutdown() {
			consumer.wakeup();
		}
		
	}

}
