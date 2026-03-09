package com.github.snuk87.keycloak.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.keycloak.Config.SystemPropertiesConfigProvider;

class KafkaProducerConfigTests {

	@AfterEach
	void cleanUp() {
		System.clearProperty("keycloak.retry.backoff.ms");
		System.clearProperty("keycloak.max.block.ms");
		System.clearProperty("keycloak.ssl.endpoint.identification.algorithm");
	}

	@Test
	void shouldReturnMapWithConfigWhenPropertyExists() {
		System.setProperty("keycloak.retry.backoff.ms", "1000");
		System.setProperty("keycloak.max.block.ms", "5000");
		System.setProperty("keycloak.foo", "bar");

		Map<String, Object> config = KafkaProducerConfig.init(new SystemPropertiesConfigProvider().scope());
		Map<String, Object> expected = Map.of("retry.backoff.ms", "1000", "max.block.ms", "5000");

		assertEquals(expected, config);
	}

	@Test
	void shouldSetEmptyStringForDisabledSSLEndpointIdentification() {
		System.setProperty("keycloak.ssl.endpoint.identification.algorithm", "disabled");

		Map<String, Object> config = KafkaProducerConfig.init(new SystemPropertiesConfigProvider().scope());

		assertEquals("", config.get("ssl.endpoint.identification.algorithm"));
	}
}
