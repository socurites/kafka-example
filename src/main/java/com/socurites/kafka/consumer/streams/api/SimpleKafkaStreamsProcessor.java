package com.socurites.kafka.consumer.streams.api;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class SimpleKafkaStreamsProcessor {
	private final static String BOOTSTRAP_SERVERS = "52.193.238.153:9092";
	
	public static void filter(final String applicationId,
			final String sourceTopic, final String targetTopic) {
		Properties configs = new Properties();
		configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		Topology topology = new Topology();
		topology.addSource("Source", sourceTopic)
			.addProcessor("Process", 
					() -> new FilterProcessor(), 
					"Source")
			.addSink("Sink", targetTopic, "Process");
		
		KafkaStreams kafkaStreams = new KafkaStreams(topology, configs);
		kafkaStreams.start();
	}
	
	public static class FilterProcessor implements Processor<String, String, String, String> {
		private ProcessorContext<String, String> context;
		
		@Override
		public void init(ProcessorContext<String, String> context) {
			Processor.super.init(context);
			
			this.context = context;
		}

		@Override
		public void process(Record<String, String> record) {
			if (record.value().length() > 5) {
				this.context.forward(record);
			}
			
			this.context.commit();
		}
	}
	
	public static void main(String[] args) {
		final String applicationId = "streams-filter-application";
		final String sourceTopic = "stream-log";
		final String targetTopic = "stream-log-copy";
		
		filter(applicationId, sourceTopic, targetTopic);
	}
}
