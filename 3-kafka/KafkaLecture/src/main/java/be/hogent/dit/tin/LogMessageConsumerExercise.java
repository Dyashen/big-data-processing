package be.hogent.dit.tin;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogMessageConsumerExercise {

	public static void main(String[] args) {
		
		
	
		// create properties for consumer and producer
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


		// create ConsumerRunnable
		final ConsumerRunnable consumer = null;
		final Thread consumerThread = new Thread(consumer);
		consumerThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdownhook called. Calling shutdown");
				consumer.shutdown();

				try {
					consumerThread.join(); // wacht tot de andere thread klaar is
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("Done waiting on consumer");
			}
		});
	}
}

class ConsumerRunnable implements Runnable {

	private final Consumer<String, String> consumer;
	private final Producer<String, String> producer;

	private final AtomicBoolean stopRequested;

	public ConsumerRunnable(Properties consumerProperties, Properties producerProperties) {
		this.consumer = new KafkaConsumer<>(consumerProperties);
		this.producer = new KafkaProducer<>(producerProperties);
		this.stopRequested = new AtomicBoolean(false);
	}

	@Override
	public void run() {
		// Hier verbruiken we berichten uit log.messages totdat de requests stoppen.
		// Alles wat moet overblijven wordt eruit gefilterd.

		try {
			while (!stopRequested.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Received record with key: " + record.key() + "and value" + record.value());
				}
			}
		} finally {
			System.out.println("About to close the consumer.");
			consumer.close();
			System.out.println("Consumer closed");
		}

	}

	public void shutdown() {
		stopRequested.set(true);
	}
}
