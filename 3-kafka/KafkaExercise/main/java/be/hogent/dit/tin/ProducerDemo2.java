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


/**
 * Producing multiple messages with a key and a value.
 * A callback is used to show the received metadata.
 * 
 * @author Stijn Lievens
 */
public class ProducerDemo2 {
	
	private static final int NUM_MESSAGES = 100;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo2.class);
	
	public static void main(String[] args) throws InterruptedException {       
	    // create Producer properties
	    Properties properties = new Properties();
	    String bootstrapServers = "127.0.0.1:9092";
	    
	    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    
	    // create the producer	
	    KafkaProducer<String, String> producer =  new KafkaProducer<>(properties);
	    
	    for (int i = 0; i < NUM_MESSAGES; i++) {
	    	// send data - asynchronous
	    	String topic = "first_topic";
	    	String key = "id_" + Integer.toString(i % 10);
	    	String message = "Hello world " + Integer.toString(i);
	    	
		    ProducerRecord<String, String> record = new ProducerRecord<>(
			    topic, key, message);
		    producer.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e == null) {
						LOGGER.info(
							"Received new metadata\n" + 
						"Topic: " + metadata.topic() + "\n" +
						"Partition: " + metadata.partition() + "\n" +
						"Offset: " + metadata.offset() + "\n" +
						"Timestamp: " + metadata.timestamp()
								);
					} else {
						LOGGER.error("Error while producing", e);
					}									
				}
		    	
		    });
		    
		    // Short delay between sending the messages
		    Thread.sleep(100);
	    }
	    	
	    // flush the producer
	    producer.flush();
	    // flush and close 
	    producer.close();     	    	    
	}

}
