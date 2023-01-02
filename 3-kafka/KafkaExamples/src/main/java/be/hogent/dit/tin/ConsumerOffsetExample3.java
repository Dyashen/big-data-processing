package be.hogent.dit.tin;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use <code>commitAsync</code> and <code>commitSync</code> to commit offsets.
 * 
 * @author Stijn Lievens
 *
 */
public class ConsumerOffsetExample3 {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);


	public static void main(String[] args) {
		
		String bootstrapServers = "localhost:9092";
		String groupId = "my-application";
		String topic = "first_topic";

		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		// Disable automatic commit
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		// Each call to poll will return at most 2 records
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2"); 
		
		// Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		// Subscribe to topic
		consumer.subscribe(Collections.singleton(topic));
		
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

				
		// Start the (infinite) poll loop. Surround with try-catch to catch the WakeupException
		try {
			while (!stopRequested.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					LOGGER.info("key: " + record.key() + ", value: " + record.value());
					LOGGER.info("Partition: " + record.partition() + ", Offset:" + record.offset());
				}
				
				try {
					Thread.sleep(2000); // Sleep for two seconds 
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				// Commit the offsets asynchronously				
				consumer.commitAsync();				
			}
		} finally {
			System.err.println("Starting to close the consumer");
			// Commit the offsets synchronously just before closing the consumer
			try {
			  consumer.commitSync();
			} catch (CommitFailedException e) {
			  LOGGER.error("Commit failed", e);	
			}
			consumer.close();
			System.err.println("Consumer closed");
		}
		
	}
}
