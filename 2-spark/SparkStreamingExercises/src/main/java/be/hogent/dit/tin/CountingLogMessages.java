package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.desc;

public class CountingLogMessages {

	private static final String INPUT_FILE = "src/main/resources/log.messages.delayed.txt";
	private static final String OUTPUT_FILE = "src/main/resources/folder";
	private static final String STREAMING_SOURCE = "src/main/resources/folder";
	private static final String STREAMING_SINK = "";
	private static final String WATERMARK_DELAY = "5 minutes";
	private static final String CHECKPOINT_LOCATION = "checkpoint_counting";

	public static void main(String[] args) {

		// van kafka lezen --> sparksession nodig

		SparkSession spark = SparkSession.builder().appName("CountingLogMessages").master("local[1]").getOrCreate();

		Dataset<Row> messagesDF = spark.read().text(INPUT_FILE);

		messagesDF = messagesDF.withColumn("message", split(col("value"), "\\s"))
				.withColumn("loglevel", element_at(col("message"), 1))
				.withColumn("system", element_at(col("message"), 2)).withColumn("date", element_at(col("message"), 3))
				.withColumn("time", element_at(col("message"), 4)).drop("message", "value")
				.withColumn("eventTime", concat_ws(" ", col("date"), col("time")))// .withColumn("eventTime",
																					// concat(col("date"), lit(" "),
																					// col("time")))
				.drop("date", "time").withColumn("window", window(col("eventTime"), "10 minutes", "5 minutes"))
				.groupBy("window", "loglevel", "system").count()
				.orderBy(col("window"), col("count").desc(), col("system"))
				.select("window.*", "loglevel", "system", "count"); // window --> start + end

		messagesDF.show(false);

		Dataset<Row> messages = spark.readStream().text(STREAMING_SOURCE);

		messages = messages.withColumn("message", split(col("value"), "\\s"))
				.withColumn("loglevel", element_at(col("message"), 1))
				.withColumn("system", element_at(col("message"), 2)).withColumn("date", element_at(col("message"), 3))
				.withColumn("time", element_at(col("message"), 4)).drop("message", "value")
				.withColumn("eventTime", concat_ws(" ", col("date"), col("time")))
				.drop("date", "time")
				.withColumn("window", window(col("eventTime"), "10 minutes", "5 minutes"))
				.withColumn("eventTime", col("eventTime").cast(DataTypes.TimestampType))
				.withWatermark("eventTime", WATERMARK_DELAY)
				.groupBy("window", "loglevel", "system")
				.count()
				.orderBy(col("window"), col("count").desc(), col("system"))
				.select("window.*", "loglevel", "system", "count"); 

		// messagesDF.write().mode(SaveMode.Overwrite).csv(OUTPUT_FILE);
		
		spark.close();

	}

}
