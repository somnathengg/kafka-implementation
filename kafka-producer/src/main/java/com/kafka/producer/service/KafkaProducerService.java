package com.kafka.producer.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

@Service
public class KafkaProducerService {
	
	private static final Logger logger = LogManager.getLogger(KafkaProducerService.class);
    
	private static final String TOPIC = "latest-topic";
	
	KafkaProducer<String, String> producer;

	@PostConstruct
	public void loadProp()
	{
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		producer = new KafkaProducer<>(configProps);
		logger.info("kafka property loaded sucessfully!!");
	}
	
	
	public boolean sendMessage(String message) {
		ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", message);
		Future<RecordMetadata> send = producer.send(record);
		try {
			if (null != send.get()) {
				logger.info("Message sent: " + message);
				return true;
			} else {
				logger.info("Error in message sending to broker : ");
				return false;
			}
		} catch (InterruptedException e) {
			return false;
		} catch (ExecutionException e) {
			return false;
		}

	}
}
