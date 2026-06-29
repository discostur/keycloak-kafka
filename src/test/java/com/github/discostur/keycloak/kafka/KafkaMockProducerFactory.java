package com.github.discostur.keycloak.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaMockProducerFactory implements KafkaProducerFactory {

	private static final Partitioner NOOP_PARTITIONER = new Partitioner() {
		@Override
		public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
			return 0;
		}
		@Override public void close() {}
		@Override public void configure(Map<String, ?> configs) {}
	};

	@Override
	public Producer<String, String> createProducer(String clientId, String bootstrapServer,
			Map<String, Object> optionalProperties) {
		return new MockProducer<String, String>(true, NOOP_PARTITIONER, new StringSerializer(), new StringSerializer());
	}

}
