package be.hogent.dit.tin;

import java.time.Duration;
import java.util.Collections;
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

/*
 * Write a Kafka Consumer LogMessageConsumerExercise that reads from the topic log.messages 
 * and filters out the messages on the log levels ERROR and FATAL and sends these to the 
 * Kafka topic important.log.messages. Make your consumer part of a consumer group. 
 * Again, use the source system as the key in your message. Make sure that your consumer is 
 * properly shut down when the JVM exits. Can you run your consumer in a separate Thread?
 * 
 *  Hint: let your consumer implement the Runnable interface and give it an additional method 
 *  shutdown that you can call from the ShutdownHook.
 */

public class ConsumeLogMessages {
	
	static final String BOOTSTRAP_SERVER = "localhost:9092";
	static final String TOPIC = "log.messages";
	static final String GROUP_ID = "important";
	
	static final Integer DELAY_MS = 1000; // 1000 ms

	public static void main(String[] args) {
		
		String GROUP_ID = ""; 
	
		// create properties for consumer and producer
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


		// create ConsumerRunnable
		final ConsumerRunnable consumer = null;
		final Thread consumerThread = new Thread(consumer);
		consumerThread.start();

		
		// Wordt aangesproken als het programma opeens wordt afgesloten.
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
		this.consumer = new KafkaConsumer<String, String>(consumerProperties);
		this.producer = new KafkaProducer<String, String>(producerProperties);
		this.stopRequested = new AtomicBoolean(false);
	}
	
	public void run() {
		// hier komt de while-loop
		try{
			while(!stopRequested.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
				for(ConsumerRecord<String, String> record : records) {
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