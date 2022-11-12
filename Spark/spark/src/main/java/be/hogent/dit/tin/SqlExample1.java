package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Simple class showing how to register a temporary view
 * and how to run a regular SQL query against this view.
 * 
 * @author Stijn Lievens
 *
 */
public class SqlExample1 {

	public static void main(String[] args) {

		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> df = spark.read()
			.option("header", true)
			.option("inferschema", true)
			.csv("src/main/resources/students.csv");
		
		// Register DataFrame as temporary view. 
		// Choose any name you want for this view.
		df.createOrReplaceTempView("student_tbl");
		
		// execute SQL-statements on the SparkSession object
		Dataset<Row> results = spark.sql(
				"SELECT year, subject, MAX(score) AS max, "
			  + "MIN(score) AS min, ROUND(AVG(score), 2) AS average "
			  + "FROM student_tbl "
			  + "GROUP BY year, subject");
		
		results.show();

	}

}
