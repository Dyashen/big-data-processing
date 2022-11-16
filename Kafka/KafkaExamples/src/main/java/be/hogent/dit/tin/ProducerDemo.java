package be.hogent.dit.tin;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Extremely simple Kafka producer that sends a single 
 * "Hello world" message to the topic "first_topic".
 * 
 * @author Stijn Lievens
 *
 */
public class ProducerDemo {
	
	public static void main(String[] args) {  // class ProducerDemo     
	    // create Producer properties
	    Properties properties = new Properties();
	    String bootstrapServers = "127.0.0.1:9092";
	    
	    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    
	    // create the producer	
	    KafkaProducer<String, String> producer =  new KafkaProducer<>(properties);
		
	    
	    // create producer record
	    ProducerRecord<String, String> record = new ProducerRecord<>(
		    "first_topic", "Hello world");
	    // send data - asynchronous
	    producer.send(record);
		
	    // flush the producer
	    producer.flush();
	    // close the producer 
	    producer.close();     	    	    
	}
}
