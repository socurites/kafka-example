package com.socurites.kafka.producer;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.socurites.kafka.partitioner.SimpleCustomPartitioner;

@Component
public class SimpleProducer {
	private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

	
	private final static String TOPIC_NAME = "test";
	private final static String BOOTSTRAP_SERVERS = "52.193.238.153:9092";
	
	private KafkaProducer<String, String> producer;
	
	@PostConstruct
	public void init() {
		Properties configs = new Properties();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Set custom partitioner
		//configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimpleCustomPartitioner.class);
		
		producer = new KafkaProducer<>(configs);
	}
	
	@PreDestroy
	public void close() {
		producer.close();
	}
	
	public void send(String key, String value) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, key, value);
		
		producer.send(record);
		log.info("Sended Records: {}", record);
		producer.flush();
	}
}