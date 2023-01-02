package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Simple class demonstrating the use of some <code>Column</code> 
 * related functions.
 * 
 * @author Stijn Lievens
 *
 */
public class ColumnExample {

	public static void main(String[] args) {

		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();

		// Read the DataFrame						
		Dataset<Row> df = spark.read()
				.option("header", true)          // file has a header
				.option("inferschema", true)     // infer the schema
				.csv("src/main/resources/small.csv");
		
		
		// do not forget to reassign. Chain method calls
		df = df.withColumn("ageNextYear", expr("age + 1"))
			   .withColumn("over30", expr("age > 30"));
		
		df.show();
					
		spark.close();
	}

}
