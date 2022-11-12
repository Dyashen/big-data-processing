package be.hogent.dit.tin;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Simple class demonstrating the use of <code>select</code> to 
 * do projection in SQL.
 * 
 * @author Stijn Lievens
 * 
 */
public class SelectExample {

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
		
		System.out.println("Prior to selecting columns: "
				+ Arrays.toString(df.columns()));

		// only keep three columns
		df = df.select("score", "quarter", "year");
		
		System.out.println("After selecting columns: "
				+ Arrays.toString(df.columns()));
				
		spark.close();
	}

}
