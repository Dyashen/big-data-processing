package be.hogent.dit.tin;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @author Dylan Cluyse
 * @description This piece of software counts to 2004 and produces the record, in a simple way, to the broker.
 */

public class ProducerDemo {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);	
	static final int NUM_MESSAGES = 2004;
	final static String TOPIC = "phantom_dust_gang";
	final static String bootstrapServers = "localhost:9092";
	
	public static void main(String[] args) throws InterruptedException {
		
	Properties properties = new Properties();
	
	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
	
	for(int i=0; i<NUM_MESSAGES; i++) 
	{
		String key = "id_" + Integer.toString(i % 10);
		String message = "Allow me to explain the numeric system: " + Integer.toString(i);
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC,  key, message);
		producer.send(record, new Callback() {
			
			// na iedere keer te versturen
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception == null) {
					LOGGER.info(
						"Received new metadata\n" + 
					"Topic: " + metadata.topic() + "\n" +
					"Partition: " + metadata.partition() + "\n" +
					"Offset: " + metadata.offset() + "\n" +
					"Timestamp: " + metadata.timestamp()
							);
				} else {
					LOGGER.error("Error while producing", exception);
				}	
			}
		});
		
		Thread.sleep(2000);
	}
	
	producer.flush();
	producer.close();
	}
}
