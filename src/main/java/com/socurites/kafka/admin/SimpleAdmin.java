package com.socurites.kafka.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SimpleAdmin {
	private static final Logger log = LoggerFactory.getLogger(SimpleAdmin.class);

	private final static String BOOTSTRAP_SERVERS = "52.193.238.153:9092";
	
	private AdminClient adminClient;
	
	@PostConstruct
	public void init() {
		Properties configs = new Properties();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		
		adminClient = AdminClient.create(configs);
	}
	
	@PreDestroy
	public void close() {
		adminClient.close();
	}
	
	public void describeCluster() throws InterruptedException, ExecutionException {
		DescribeClusterResult clusterResult = adminClient.describeCluster();
		KafkaFuture<Collection<Node>> nodesFuture = clusterResult.nodes();
		Collection<Node> nodes = nodesFuture.get();
		
		for(Node node : nodes) {
			ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
			
			DescribeConfigsResult configResult = adminClient.describeConfigs(Collections.singleton(configResource));
			
			KafkaFuture<Map<ConfigResource, Config>> resourceFuture = configResult.all();
			Map<ConfigResource, Config> resourceMap = resourceFuture.get();
			
			resourceMap.forEach((resource, config) -> {
				Collection<ConfigEntry> configEntries = config.entries();
				configEntries.forEach(configEntry -> log.info(
						"[" + node.idString() + "]" +
						configEntry.name() + "=" + configEntry.value()));
			});
			
			log.info("====================================\n");
		}
	}
}
