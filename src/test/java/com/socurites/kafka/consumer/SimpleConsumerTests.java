package com.socurites.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class SimpleConsumerTests {
	@Autowired
	private SimpleConsumer consumer;
	
	@Test
	public void poll() {
		consumer.poll();
	}
}
