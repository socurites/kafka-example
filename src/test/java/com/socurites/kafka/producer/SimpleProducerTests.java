package com.socurites.kafka.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class SimpleProducerTests {
	@Autowired
	private SimpleProducer producer;
	
	@Test
	public void sendWithNullKey() {
		String messageValue = "testMessage";
		producer.send(null, messageValue);
	}
	
	@Test
	public void sendWithKey() {
		String messageKey = "testKey";
		String messageValue = "Pangyo";
		producer.send(null, messageValue);
	}
};