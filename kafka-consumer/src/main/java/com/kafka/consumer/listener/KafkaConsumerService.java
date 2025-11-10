package com.kafka.consumer.listener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
@Service
public class KafkaConsumerService {

	private static final Logger logger = LogManager.getLogger(KafkaConsumerService.class);
	
	@KafkaListener(topics = "latest-topic", groupId = "group_id")
	public void consume(ConsumerRecord<String, String> record) {
		try {
			String message = record.value();
	        int partition = record.partition();
	        String topic = record.topic();
	        long offset = record.offset();
	        logger.info("Message Received-->"+message+"  Topic----->"+topic+" Partition----->"+partition+" Offset----->"+offset);
		} catch (Exception e) {
			logger.info("Exception in Storing Kafka Message in Database !!!" + e.getMessage());
		}

	}

}
