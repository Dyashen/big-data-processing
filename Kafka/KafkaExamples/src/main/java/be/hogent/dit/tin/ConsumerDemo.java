package be.hogent.dit.tin;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very simple Kafka consumer. Simply logs the received records.
 * This consumer is **not** shut down properly.
 * 
 * @author Stijn Lievens
 */
public class ConsumerDemo {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);


	public static void main(String[] args) {
		
		String bootstrapServers = "localhost:9092";
		String groupId = "my-application";

		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		// Subscribe to topic
		consumer.subscribe(Collections.singleton("first_topic"));
		
		// Start the (infinite) poll loop
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> record : records) {
				LOGGER.info("key: " + record.key() + ", value: " + record.value());
				LOGGER.info("Partition: " + record.partition() + ", Offset:" + record.offset());
			}
		}
		
	}

}
