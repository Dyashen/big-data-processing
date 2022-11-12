package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Simple class showing how to register and use a 
 * User Defined Function.
 * 
 * @author slie742
 *
 */
public class UDFExample {

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
		
		df = df.select("subject", "year", "grade")
				.withColumn("has_passed", 
				   functions.callUDF("hasPassed", col("grade")));
		
		df.show(5);
				
		spark.close();
	}

}
