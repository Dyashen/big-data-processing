package be.hogent.dit.tin;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple example of using a <code>DataFrameReader</code>.
 * 
 * @author Stijn Lievens
 *
 */
public class DataFrameReadExample {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();

		// The data types of this data
		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age",  DataTypes.IntegerType, true));

		// Create the schema
		StructType schema = DataTypes.createStructType(fields);

		// Read the DataFrame						
		Dataset<Row> df = spark.read()
				.option("header", true) // file has a header
				.schema(schema)         // set the schema explicitly
				.csv("src/main/resources/small.csv");

		// Work with the DataFrame
		Dataset<Row> result = df.groupBy("name").avg("age");

		// Show the result
		result.show();

		spark.close();
	}

}
