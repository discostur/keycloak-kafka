package com.github.discostur.keycloak.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaEventListenerProvider implements EventListenerProvider {

	private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

	private String topicEvents;

	private List<EventType> events;

	private String topicAdminEvents;

	private Producer<String, String> producer;

	private ObjectMapper mapper;

	public KafkaEventListenerProvider(String bootstrapServers, String clientId, String topicEvents, String[] events,
			String topicAdminEvents, Map<String, Object> kafkaProducerProperties, KafkaProducerFactory factory) {
		this.topicEvents = topicEvents;
		this.events = new ArrayList<>();
		this.topicAdminEvents = topicAdminEvents;

		for (String event : events) {
			try {
				EventType eventType = EventType.valueOf(event.toUpperCase());
				this.events.add(eventType);
			} catch (IllegalArgumentException e) {
				LOG.warnf("Ignoring unknown event type '%s'. Check the configured events / KAFKA_EVENTS list.", event);
			}
		}

		if (this.events.isEmpty()) {
			LOG.warn("No valid event types configured; no user events will be produced to Kafka.");
		}

		producer = factory.createProducer(clientId, bootstrapServers, kafkaProducerProperties);
		mapper = new ObjectMapper();
	}

	private void produceEvent(String eventAsString, String topic) {
		LOG.debugf("Produce to topic: %s ...", topic);
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventAsString);
		producer.send(record, (metadata, exception) -> {
			if (exception != null) {
				LOG.errorf(exception, "Failed to send event to Kafka topic %s", topic);
			} else {
				LOG.debugf("Produced to topic: %s", metadata.topic());
			}
		});
	}

	@Override
	public void onEvent(Event event) {
		if (event != null && events.contains(event.getType())) {
			try {
				produceEvent(mapper.writeValueAsString(event), topicEvents);
			} catch (JsonProcessingException e) {
				LOG.error("Failed to serialize event to JSON", e);
			}
		}
	}

	@Override
	public void onEvent(AdminEvent event, boolean includeRepresentation) {
		if (event != null && topicAdminEvents != null) {
			try {
				produceEvent(mapper.writeValueAsString(event), topicAdminEvents);
			} catch (JsonProcessingException e) {
				LOG.error("Failed to serialize admin event to JSON", e);
			}
		}
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}
}
