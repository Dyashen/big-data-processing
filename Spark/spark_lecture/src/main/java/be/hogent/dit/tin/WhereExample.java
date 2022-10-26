package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Simple class showing the use of <code>where</code> to filter
 * specific rows from the DataFrame.
 * 
 * @author Stijn Lievens
 */
public class WhereExample {

	public static void main(String[] args) {

		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> df = spark.read()
			.option("header", true)
			.option("inferschema", true)
			.csv("src/main/resources/students.csv");
		
		df.show(5); // show 5 lines
		
		// Select students with grade "A+"
		Dataset<Row> dfAplus = df.select("student_id", "year", "subject", "grade")
		  .where(col("grade").equalTo("A+"));
						
		dfAplus.show(5);
		
		// Select students with grade B in the year 2010 or 2011
		Dataset<Row> dfB1011 = df.select("student_id", "year", "subject", "grade")
		  .where(col("grade").equalTo("B")
				  .and(col("year").isin(2010, 2011)));
		
		dfB1011.show(5);
				
		// Select distinct students with grade B in any course the year 2010 or 2011
		Dataset<Row> dfDistinct = 
				df.select("student_id", "year", "grade")
				  .where(col("grade").equalTo("B")
				  .and(col("year").isin(2010, 2011)))
				  .distinct();
				
		dfDistinct.show(5);
		
	}

}
