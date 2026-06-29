package com.github.discostur.keycloak.kafka;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test: produce a Keycloak event through the real {@link KafkaStandardProducerFactory}
 * to a real Kafka broker (Testcontainers) and consume it back. The whole class is skipped when no
 * Docker daemon is available, so {@code mvn verify} stays green in environments without Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaEventListenerProviderIT {

	private static final String TOPIC = "keycloak-events";

	@Container
	private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));

	@Test
	void shouldProduceEventToRealBroker() {
		KafkaEventListenerProvider listener = new KafkaEventListenerProvider(
				KAFKA.getBootstrapServers(), "keycloak-kafka-it", TOPIC, new String[] { "REGISTER" },
				null, Map.of(), new KafkaStandardProducerFactory());

		Event event = new Event();
		event.setType(EventType.REGISTER);
		event.setRealmId("test-realm");
		event.setUserId("user-123");

		listener.onEvent(event);
		// close() flushes pending sends and releases the producer.
		listener.close();

		List<String> values = consumeAll(Duration.ofSeconds(20));

		assertFalse(values.isEmpty(), "expected at least one event on topic " + TOPIC);
		assertTrue(values.get(0).contains("REGISTER"), "payload should contain the event type: " + values.get(0));
		assertTrue(values.get(0).contains("user-123"), "payload should contain the user id: " + values.get(0));
	}

	private List<String> consumeAll(Duration timeout) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "keycloak-kafka-it-consumer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		List<String> values = new ArrayList<>();
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singletonList(TOPIC));
			long deadline = System.currentTimeMillis() + timeout.toMillis();
			while (values.isEmpty() && System.currentTimeMillis() < deadline) {
				for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
					values.add(record.value());
				}
			}
		}
		return values;
	}
}
