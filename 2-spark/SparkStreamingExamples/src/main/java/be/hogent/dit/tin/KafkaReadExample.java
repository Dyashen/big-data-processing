package be.hogent.dit.tin;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Simple class to show how to read from a Kafka topic 
 * using Spark Structured Streaming.
 * 
 * The messages read from the Kafka topic are shown on the console.
 * We assume that the Kafka broker is running on localhost:9092
 * 
 * @author Stijn Lievens
 */
public class KafkaReadExample {
	
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String CHECKPOINT_LOCATION = "checkpoint_kafkareadexample";

	public static void main(String[] args)  {
		if (args.length == 0) {
			System.out.println("Usage: KafkaReadExample <topic>");
			System.exit(-1);
		}
		
		System.setProperty("hadoop.home.dir", "C:\\tmp\\winutils-extra\\hadoop");
	
		final String topic = args[0];
		
		System.out.println("Will read from the topic " + topic);
		
		SparkSession spark = SparkSession.builder()
				.appName("KafkaReadExample")				
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> messages = spark.readStream()
			.format("kafka")
			.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
			.option("subscribe", topic)
			.load();
		
		System.out.println("Stream loaded");
		
		// Cast the key and the value to Strings
		messages = messages.withColumn("key", expr("CAST(key AS STRING)"))
		                   .withColumn("value", expr("CAST(value AS STRING)"));
		
		
		System.out.println("About to start the query");
		
		StreamingQuery query = null;
		try {
			query = messages.writeStream()
				.format("console")
				.outputMode(OutputMode.Append())
				.option("checkpointLocation", CHECKPOINT_LOCATION)
				.start();
		} catch (TimeoutException e) {		
			e.printStackTrace();
		}
				
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
		 
		spark.close();
	}
}
