package be.hogent.dit.tin;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProduceLogMessages {
	
	static final String BOOTSTRAP_SERVER = "localhost:9092";
	static final String TOPIC = "log.messages";
	static final Integer DELAY_MS = 1000; // 1000 ms

	public static void main(String[] args) {
		
		Properties properties = new Properties();
		
		final AtomicBoolean stopRequested = new AtomicBoolean(false);
		final Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdownhook called");
				stopRequested.set(true);
				try {
					mainThread.join();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		// Bootstrap-server instellen
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		
		//Key & Value serializer
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		final Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		final LogMessageGenerator generator = new LogMessageGenerator();
		
		try {
			while (!stopRequested.get()) {
				String msg = generator.next(); // volgende line offset in een bestand
				String[] parts = msg.split(" "); // message opsplitsen in een array
				String sourceSystem = parts[1]; // want de key is het tweede element in de string
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, sourceSystem, msg);
				producer.send(record); 
				try {
					Thread.sleep(DELAY_MS);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} finally {
			System.out.println("Closing the producer...");
			producer.close();
			System.out.println("Closed the producer");
		}
	}

}
