package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Read from a socket on localhost and perform a word count.
 * Output everything in Complete mode to the console.
 * 
 * First start a Netcat server on port 9999 and then run this
 * java program.
 * 
 * @author Stijn Lievens
 */
public class StructuredStreamingExample1 {

	public static void main(String[] args) throws TimeoutException {
	
		System.setProperty("hadoop.home.dir", "C:\\tmp\\winutils-extra\\hadoop");
								
		// read delay from command line if specified
		int delay = args.length == 0 ? 1 : Integer.parseInt(args[0]);
		

		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.appName("StructuredStreamingExample1")
				.getOrCreate();
		
		// The job seems terribly slow with the default number of partitions
		spark.conf().set("spark.sql.shuffle.partitions", 4);

			
		Dataset<Row> lines = spark.readStream()
			.format("socket")              // read from socket
			.option("host", "localhost")   // obvious options
			.option("port", 9999)
			.load();                       // load stream into DataFrame
		
		
		// Regular transformation split line, put each word in 
		// new record (explode) 
		Dataset<Row> words = lines.select(
				explode(split(col("value"), "\\s")).as("word"));
		Dataset<Row> counts = words.groupBy("word").count();
		
		// Write the results
		DataStreamWriter<Row> writer = counts.writeStream()
		  .format("console") // write to the console
		  .outputMode(OutputMode.Complete()) // write everything
		  .trigger(Trigger.ProcessingTime(delay, TimeUnit.SECONDS));
		
		// start execution of writer
		StreamingQuery streamingQuery = writer.start();   
		
					
		try {
			streamingQuery.awaitTermination();
		} catch (StreamingQueryException e) {			
			e.printStackTrace();
		}		
	}

}
