package com.socurites.kafka.admin;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class SimpleAdminTests {
	@Autowired
	private SimpleAdmin admin;
	
	@Test
	public void describeCluster() throws InterruptedException, ExecutionException {
		admin.describeCluster();
	}
}
