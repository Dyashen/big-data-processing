package be.hogent.dit.tin;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class SparkCountLogMessages {
	
	private static final String SERVERS = "localhost:9092";
	private static final String CHECKPOINT_LOCATION = "checkpoint_filterlogs";
	private static final String INPUT_TOPIC = "log.messages";
	private static final String OUTPUT_TOPIC = "important.log.messages";
	private static final String HADOOP_DIR = "C:\\winutils-extra\\hadoop";
	private static final String INPUT_FILE_PATH = "src/main/resources/log.messages.delayed.txt";

	private static final String WATERMARK_DELAY = "3 minutes";
	
	private static final String STATIC_OUTPUT = "src/main/resources/static-report";
	private static final String STREAMING_SOURCE = "src/main/resources/streaming-source";
	private static final String STREAMING_OUTPUT = "src/main/resources/streaming-report";

	public static void main(String[] args) throws TimeoutException, StreamingQueryException {

System.setProperty("hadoop.home.dir", HADOOP_DIR);
		
		SparkSession spark = SparkSession.builder()
				.master("local[1]")
				.appName("SparkCountLog")
				.getOrCreate();
		
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> messages = spark.read().text(INPUT_FILE_PATH);
		
		// Voor static-report.
		messages = messages
			.withColumn("value", split(col("value"),"\\s"))
			.withColumn("log-level", element_at(col("value"), 1))
			.withColumn("systeem", element_at(col("value"), 2))
			.withColumn("dag", element_at(col("value"), 3))
			.withColumn("tijd", element_at(col("value"), 4))
			.withColumn("eventtime", concat(col("dag"), lit(" "), col("tijd")))
			.select("log-level", "systeem", "eventtime")
			.withColumn("window", window(col("eventtime"), "10 minutes", "5 minutes"))
			.groupBy(col("window"), col("systeem"), col("log-level"))
			.count()
			.orderBy(col("window"), desc("count"))
			.select("window.*", "systeem", "log-level", "count");
		
		
		messages.show(false);
		
		// Uitschrijven naar file.
		messages.repartition(1)
				.write()
				.mode(SaveMode.Overwrite)
				.csv(STATIC_OUTPUT);
		
		Dataset<Row> messagesStreaming = spark.readStream().text(STREAMING_SOURCE);
		
		// Watermark toevoegen
		messagesStreaming = messagesStreaming
				.withColumn("value", split(col("value"),"\\s"))
				.withColumn("log-level", element_at(col("value"), 1))
				.withColumn("systeem", element_at(col("value"), 2))
				.withColumn("dag", element_at(col("value"), 3))
				.withColumn("tijd", element_at(col("value"), 4))
				.withColumn("eventtime", concat(col("dag"), lit(" "), col("tijd")))
				.select("log-level", "systeem", "eventtime")
				// String-types werken niet voor watermarks, dus casten.
				.withColumn("eventtime", col("eventtime").cast(DataTypes.TimestampType))
				.withWatermark("eventtime",  WATERMARK_DELAY)
				.withColumn("window", window(col("eventtime"), "10 minutes", "5 minutes"))
				.groupBy(col("window"), col("systeem"), col("log-level"))
				.count()
				// Sorteren kan enkel bij complete output!
				//.orderBy(col("window"), desc("count"))
				.select("window.*", "systeem", "log-level", "count");
		

		
		// Watermark is verplicht!!
		StreamingQuery query = messagesStreaming
				.writeStream()
				.format("csv")
				.option("path", STREAMING_OUTPUT)
				.outputMode(OutputMode.Append())
				.option("checkpointLocation", CHECKPOINT_LOCATION)
				.trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
				.start();
		
		query.awaitTermination();
		
		spark.close();
		
	}

}
