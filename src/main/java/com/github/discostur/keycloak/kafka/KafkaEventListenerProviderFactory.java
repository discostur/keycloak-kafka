package com.github.discostur.keycloak.kafka;

import java.util.Map;

import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

	private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);
	private static final String ID = "kafka";

	private KafkaEventListenerProvider instance;

	private String bootstrapServers;
	private String topicEvents;
	private String topicAdminEvents;
	private String clientId;
	private String[] events;
	private Map<String, Object> kafkaProducerProperties;

	@Override
	public EventListenerProvider create(KeycloakSession session) {
		return instance;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void init(Scope config) {
		LOG.info("Init kafka module ...");
		topicEvents = config.get("topicEvents", System.getenv("KAFKA_TOPIC"));
		clientId = config.get("clientId", System.getenv("KAFKA_CLIENT_ID"));
		bootstrapServers = config.get("bootstrapServers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
		topicAdminEvents = config.get("topicAdminEvents", System.getenv("KAFKA_ADMIN_TOPIC"));

		String eventsString = config.get("events", System.getenv("KAFKA_EVENTS"));

		if (eventsString != null) {
			events = eventsString.split(",");
		}

		if (topicEvents == null) {
			throw new NullPointerException("topic must not be null.");
		}

		if (clientId == null) {
			throw new NullPointerException("clientId must not be null.");
		}

		if (bootstrapServers == null) {
			throw new NullPointerException("bootstrapServers must not be null");
		}

		if (events == null || events.length == 0) {
			events = new String[1];
			events[0] = "REGISTER";
		}

		kafkaProducerProperties = KafkaProducerConfig.init(config);

		// Build the provider eagerly: config is fully resolved here and Keycloak calls init() once
		// at startup. This makes create() a cheap, thread-safe getter (no lazy double-checked init
		// race) and fails fast if the Kafka producer cannot be constructed.
		instance = new KafkaEventListenerProvider(bootstrapServers, clientId, topicEvents, events, topicAdminEvents,
				kafkaProducerProperties, new KafkaStandardProducerFactory());
	}

	@Override
	public void postInit(KeycloakSessionFactory arg0) {
		// ignore
	}

	@Override
	public void close() {
		if (instance != null) {
			instance.close();
		}
	}
}
