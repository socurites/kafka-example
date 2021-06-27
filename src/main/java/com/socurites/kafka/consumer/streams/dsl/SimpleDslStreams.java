package com.socurites.kafka.consumer.streams.dsl;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDslStreams {
	private static final Logger log = LoggerFactory.getLogger(SimpleDslStreams.class);

	private final static String BOOTSTRAP_SERVERS = "52.193.238.153:9092";
	
	public static void startCopyStreams(final String applicationName, final String fromTopic, final String toTopic) {
		Properties configs = new Properties();
		configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
		configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		streamsBuilder.stream(fromTopic)
			.to(toTopic);
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), configs);
		kafkaStreams.start();
	}
	
	public static void main(String[] args) {
		final String applicationName = "streams-application";
		final String fromTopic = "stream-log";
		final String toTopic = "stream-log-copy";
		
		startCopyStreams(applicationName, fromTopic, toTopic);
	}
}
