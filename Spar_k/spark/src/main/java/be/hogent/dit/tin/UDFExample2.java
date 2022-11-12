package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Simple class showing how to use a UDF as a regular SQL function.
 * 
 * @author Stijn Lievens
 *
 */
public class UDFExample2 {

	public static void main(String[] args) {

		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> df = spark.read()
			.option("header", true)
			.option("inferschema", true)
			.csv("src/main/resources/students.csv");
		
		// Register the UDF:
		// 1. udf() gives access to UDFRegistration
		// 2. register method has three arguments:
		//    a. The name you will give to this method
		//    b. The actual implementation (in this case using lambda-syntax)
		//    c. The return type of the method.
		spark.udf().register(
				"hasPassed", 
				(String grade) -> { return grade.startsWith("A") || 
							grade.startsWith("B") || grade.startsWith("C");
				                  }, 
				DataTypes.BooleanType);
		
		df.createOrReplaceTempView("student_tbl");
		
		Dataset<Row> result = spark.sql(
			  "SELECT subject, year, grade, hasPassed(grade) AS has_passed "
			+ "FROM student_tbl");
		
		result.show(5);
		
		spark.close();		
	}

}
