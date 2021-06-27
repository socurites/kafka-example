package com.socurites.kafka.consumer.streams.dsl;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDslJoinKtable {
	private static final Logger log = LoggerFactory.getLogger(SimpleDslJoinKtable.class);

	private final static String BOOTSTRAP_SERVERS = "52.193.238.153:9092";
	
	public static void join(final String applicationId, final String kTableName,
			final String sourceTopic, final String targetTopic) {
		Properties configs = new Properties();
		configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KTable<String, Object> kTable = streamsBuilder.table(kTableName);
		KStream<String, String> sourceStream = streamsBuilder.stream(sourceTopic);
		
		sourceStream.join(kTable, 
				(s, r) -> s + " send to " + r)
			.to(targetTopic);
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), configs);
		kafkaStreams.start();
	}
	
	public static void main(String[] args) {
		final String applicationId = "ktable-join-application";
		final String sourceTopic = "stream-log";
		final String kTableName = "address";
		final String targetTopic = "stream-log-copy";
		
		join(applicationId, kTableName, sourceTopic, targetTopic);
	}
}
