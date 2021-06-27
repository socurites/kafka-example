package com.socurites.kafka.consumer.multiworker;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiWorkerConsumer {
	private static final Logger log = LoggerFactory.getLogger(MultiWorkerConsumer.class);

	private final static String TOPIC_NAME = "test";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String GROUP_ID = "test-group";
	
	private KafkaConsumer<String, String> kafkaConsumer;
	
	private ExecutorService executorService = Executors.newCachedThreadPool();
	
	public void init() {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		
		kafkaConsumer = new KafkaConsumer<>(configs);
		kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
	}
	
	public void poll() {
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
			
			for(ConsumerRecord<String, String> record : records) {
				executorService.execute(() -> {						// <- Consume Worker Thread
					log.info("Consumed: {}", record);
				});
			}
		}
	}
	
	public static void main(String[] args) {
		MultiWorkerConsumer consumer = new MultiWorkerConsumer();
		consumer.init();
		
		consumer.poll();
	}
}