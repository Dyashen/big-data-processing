package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class WordCount {
	
	static SparkSession spark;
	
	static final String INPUT_PATH = "src/main/resources/textfile.txt";

	public static void main(String[] args) {

		spark = SparkSession.builder().appName("WordCount").master("local[1]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> wordCount = spark.read()
									.text(INPUT_PATH);
		
		wordCount.withColumn("woorden", explode(split(col("value"), " ")))
				.groupBy(col("woorden"))
				.agg(count("woorden").as("aantalWoorden"))
				.orderBy(desc("aantalWoorden"))
				.show();
	}

}
