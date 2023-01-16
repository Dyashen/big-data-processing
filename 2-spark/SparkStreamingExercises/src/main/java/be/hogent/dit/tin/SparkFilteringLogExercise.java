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

import static org.apache.spark.sql.functions.col;

public class SparkFilteringLogExercise {
	
	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	private static final String INPUT_TOPIC = "log.messages";
	private static final String OUTPUT_TOPIC = "important.log.messages";
	private static final String CHECKPOINT_LOCATION = "checkpoint_kafkafilter";

	public static void main(String[] args) throws TimeoutException, StreamingQueryException {
		
		// van kafka lezen --> sparksession nodig
		
		SparkSession spark = SparkSession.builder().appName("SparkFilteringLogExercise").master("local[1]").getOrCreate();
		
		System.setProperty("hadoop.home.dir", "C:\\winutils-extra\\hadoop");
		
		Dataset<Row> messages = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
				.option("subscribe", INPUT_TOPIC)
				.load();
		
		messages = messages //filteren
				.select("key","value")
				.withColumn("value", col("value").cast(DataTypes.StringType))
				.filter(col("value").like("ERROR%").or(col("value").like("FATAL%")));
		
		StreamingQuery query = messages.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
				.option("topic", OUTPUT_TOPIC)
				.outputMode(OutputMode.Append()) // enkel nieuwe rijen, 
				.trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
				.option("checkpointLocation", CHECKPOINT_LOCATION)
				.start(); // verversen iedere vijf seconden
		
		query.awaitTermination();
		
		spark.close();

	}

}
