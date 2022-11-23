//java -cp KafkaLecture-0.0.1-SNAPSHOT-jar-with-dependencies.jar be.hogent.dit.tin.KafkaProducerDemo

package be.hogent.dit.tin;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	
	private static final String TOPIC = "lecture";
	private static final int NUM_MESSAGES = 50;
	
	public static void main(String[] args) {
	
		// Set the properties for the Kafka producer.
		/*
		 * 1. Linken met de juiste
		 * 2. Serialization: moet niet dezelfde serializer zijn.
		 * 
		 */
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");  
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create the Kafka producer
		Producer<String, String> producer = new KafkaProducer<>(properties);
		
		// Create a ProducerRecord
		/*
		 * Bericht versturen naar een topic.
		 * 1. Topic string maken (hierboven).
		 * 
		 */ 
		for (int i=0; i < NUM_MESSAGES; i++){
			
			String key = "key_" + (i%10); //hashing nodig om in de juiste partitie terecht te komen.
			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, i + "I've got an announcement -- uhh -- hiya!!!");

			// Send the record + callback meegeven
			producer.send(record, new Callback() {
				
				
				// Exception.
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						System.out.println("Got Metadata" + "partition: " + metadata.partition() + " offset: " + metadata.offset() + " timestamp: " + metadata.timestamp() + " topic: " + metadata.topic());
					} else {
						System.out.println("Error!" + exception);
					}
				}
			});
		}
		
		// Close producer
		/*
		 * flush() --> 
		 * close() --> 
		 */
		producer.flush();
		producer.close();
		
	}

}
