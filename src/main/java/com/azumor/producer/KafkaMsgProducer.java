package com.azumor.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.protobuf.Message;

public class KafkaMsgProducer {
	/** HOST and PORT of Kafka Server */
	private static final String HOST = System.getProperty("KAFKA_HOST", "localhost:9092");

	/** Connect producer */
	private static final Properties configProperties;
	static {
		// Configure the Producer
		configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

	}

	/**
	 * This method publish the message to Kafka Server.
	 * 
	 * @param message
	 */
	public static synchronized void publish(Message message, String topicName) {
		KafkaProducer<String, String> producer = null;
		try {
			producer = new KafkaProducer<String, String>(configProperties);

			ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, message.toString());
			producer.send(rec);
		} finally {
			if (null != producer) {
				producer.close();
			}
		}

	}

}
