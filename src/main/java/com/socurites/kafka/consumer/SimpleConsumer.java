package com.socurites.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SimpleConsumer {
	private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

	private final static String TOPIC_NAME = "test";
	private final static String BOOTSTRAP_SERVERS = "52.193.238.153:9092";
	private final static String GROUP_ID = "test-group";
	
	KafkaConsumer<String, String> consumer;
	
	@PostConstruct
	public void init() {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		
		consumer = new KafkaConsumer<>(configs);
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
	}
	
	public void poll() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
			
			for(ConsumerRecord<String, String> record : records) {
				log.info("Consumed: {}", record);
			}
		}
	}
}