package be.hogent.dit.tin;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;


public class ReDo_FilterLogMessages {
	
	private static final String SERVERS = "localhost:9092";
	private static final String CHECKPOINT_LOCATION = "checkpoint_filterlogs";
	private static final String INPUT_TOPIC = "log.messages";
	private static final String OUTPUT_TOPIC = "important.log.messages";
	private static final String HADOOP_DIR = "C:\\winutils-extra\\hadoop";

	public static void main(String[] args) {
		
		// Nodig voor configuratie
		System.setProperty("hadoop.home.dir", HADOOP_DIR);
		
		// Spark-object plus config
		SparkSession spark = SparkSession.builder()
				.master("local[1]")
				.appName("SparkFilterLog")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> messages = spark.readStream()
					.format("kafka")
					.option("kafka.bootstrap.servers", SERVERS)
					.option("subscribe", INPUT_TOPIC)
					.load();
		
		messages = messages
				.withColumn("value", col("value").cast(DataTypes.StringType))
				.where(col("value").like("ERROR%")
						.or(col("value").like("FATAL%")))
				// Aantal uitprinten
				.withColumn("type", element_at(split(col("value"), " "), 1))
				.select("key","value");
				
		
		// Aantal uitprinten
		
		StreamingQuery query = null;
		
		try {
			query = messages.writeStream()
					.format("kafka")
					.option("kafka.bootstrap.servers", SERVERS)
					.option("topic", OUTPUT_TOPIC)
					.option("checkpointLocation", CHECKPOINT_LOCATION)
					.outputMode(OutputMode.Update())
					.trigger(Trigger.ProcessingTime(15, TimeUnit.SECONDS))
					.start();
		} catch(TimeoutException e) {
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
