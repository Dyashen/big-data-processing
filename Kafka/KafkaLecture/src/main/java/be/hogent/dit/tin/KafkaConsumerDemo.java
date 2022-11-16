//java -cp KafkaLecture-0.0.1-SNAPSHOT-jar-with-dependencies.jar be.hogent.dit.tin.KafkaConsumerDemo

package be.hogent.dit.tin;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.datatype.DatatypeConstants.Field;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerDemo {

	private static final String GROUP_ID = "lecture-app";
	private static final String TOPIC = "lecture";
	

	public static void main(String[] args) {

		final AtomicBoolean stopRequested = new AtomicBoolean(false);
		final Thread mainThread = Thread.currentThread();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				stopRequested.set(true);
				
				try {
					mainThread.join(); //blokeert tot de main thread klaar is
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					System.out.println("Done waiting for main thread");
				}
			}

		});

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// create consumer
		Consumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Collections.singleton(TOPIC));

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

}
