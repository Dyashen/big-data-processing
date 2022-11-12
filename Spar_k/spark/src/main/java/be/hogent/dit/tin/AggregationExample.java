package be.hogent.dit.tin;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Simple class showing the use of some aggregation functions.
 * 
 * @author Stijn Lievens
 *
 */
public class AggregationExample {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();
					
		Dataset<Row> df = spark.read()
			.option("header", true)
			.option("inferschema", true)
			.csv("src/main/resources/students.csv");
		
		// Example one: count the number of exams in each year.
		//              order the results by count, descending
		
		df.select("year").groupBy("year")
			.count().orderBy(desc("count")).show();
		
		// Example two: compute max, min and average score for each year/subject.
		
		df.select("year", "subject", "score").groupBy("year", "subject")
			.agg(max("score"), min("score"), avg("score")).show();
		
		// Example three: same, but clean up the report
		
		df.select("year", "subject", "score").groupBy("year", "subject")
			.agg(max("score").alias("max"), 
				 min("score").alias("min"),
				 round(avg("score"),2).alias("average")).show();

	}

}
