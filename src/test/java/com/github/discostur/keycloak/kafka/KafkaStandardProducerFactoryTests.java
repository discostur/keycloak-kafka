package com.github.discostur.keycloak.kafka;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class KafkaStandardProducerFactoryTests {

	@Test
	void shouldCreateProducer() {
		KafkaStandardProducerFactory factory = new KafkaStandardProducerFactory();
		Producer<String, String> producer = factory.createProducer("test-client", "localhost:9092", Map.of());
		assertNotNull(producer);
		producer.close(Duration.ofMillis(100));
	}

	@Test
	void shouldApplyOptionalProperties() {
		KafkaStandardProducerFactory factory = new KafkaStandardProducerFactory();
		Map<String, Object> extra = Map.of(ProducerConfig.ACKS_CONFIG, "all");
		Producer<String, String> producer = factory.createProducer("test-client", "localhost:9092", extra);
		assertNotNull(producer);
		producer.close(Duration.ofMillis(100));
	}
}
