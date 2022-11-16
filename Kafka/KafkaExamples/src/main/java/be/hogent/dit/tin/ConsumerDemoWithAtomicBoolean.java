package be.hogent.dit.tin;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properly shutting down a single consumer running in the main thread
 * using a <code>shutDownHook</code> and an atomic boolean to indicate 
 * that we want to stop the consumer.
 * 
 * @author Stijn Lievens
 *
 */
public class ConsumerDemoWithAtomicBoolean {
	
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
		
		// Add the shutdown hook
		final Thread mainThread = Thread.currentThread();
		final AtomicBoolean stopRequested = new AtomicBoolean(false);
		LOGGER.info("mainThread has name: " + mainThread.getName());
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.err.println("Starting exit .....");
				LOGGER.info("shutDownHook running in thread: " + Thread.currentThread().getName());
				stopRequested.set(true);
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

				
		// Start the (infinite) poll loop.  Close the consumer in the finally block. 
		try {
			while (!stopRequested.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					LOGGER.info("key: " + record.key() + ", value: " + record.value());
					LOGGER.info("Partition: " + record.partition() + ", Offset:" + record.offset());
				}
			}
		} finally {
			System.err.println("Starting to close the consumer");
			consumer.close();
			System.err.println("Consumer closed");
		}
		
	}

}
