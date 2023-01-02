package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.window;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Simple class to demonstrate windowed aggregation.
 * Spark reads from a socket and adds a time stamp.
 * This time stamp is then used to aggregate word counts into intervals
 * with a width of 10 seconds. These intervals slide forward every 5 seconds. 
 * 
 * The complete result table is output to the console every 5 seconds.
 * Note: this program will accumulate unbounded state when running indefinitely
 * since it doesn't use a watermark delay and uses 'complete' output mode. 
 * 
 * Data is read from localhost port 9999. First start a Netcat server on port 9999.
 * 
 * @author Stijn Lievens
 */
public class WindowedAggregationExample {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tmp\\winutils-extra\\hadoop");
		
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.appName("WindowedAggregationExample")
				.getOrCreate();
		
		// The job seems terribly slow with the default number of partitions
		spark.conf().set("spark.sql.shuffle.partitions", 4);
		
		Dataset<Row> lines = spark.readStream()
				.format("socket")              // read from socket
				.option("host", "localhost")   // obvious options
				.option("port", 9999)
			    .option("includeTimestamp", true) // add timestamp column
			    .load();                       // load stream into DataFrame
		
		// lines DataFrame has columns "value" and "timestamp"
		Dataset<Row> output = lines
				.withColumn("word", explode(split(col("value"), "\\s")))
				.select("word", "timestamp")
				.groupBy(
					    window(col("timestamp"), "10 seconds", "5 seconds"),
						col("word"))
				.count()
				.orderBy(col("window"), col("count"));
		
		StreamingQuery query = null;
		
		
		// Output to the console.
		try {
			query = output.writeStream()
				.format("console")
				.option("truncate", false)
				.option("numRows", 100)
				.outputMode(OutputMode.Complete())
				.trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
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
