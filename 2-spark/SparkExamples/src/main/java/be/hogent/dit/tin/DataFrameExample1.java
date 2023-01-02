package be.hogent.dit.tin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Small class demonstrating how to create in memory data, 
 * how to specify a schema programmatically and how to create 
 * a DataFrame from the in memory data and the schema.
 * 
 * @author Stijn Lievens
 */
public class DataFrameExample1 {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
				.appName("example").master("local[*]")
				.getOrCreate();
		
		// Create some in memory data
		List<Row> inMemory = new ArrayList<>();
		
		inMemory.add(RowFactory.create("Brooke", 20));
		inMemory.add(RowFactory.create("Brooke", 25));
		inMemory.add(RowFactory.create("Denny", 31));
		inMemory.add(RowFactory.create("Jules", 30));
		inMemory.add(RowFactory.create("TD", 35));
		
		// The data types of this data
		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true)
				);
		
		// Create the schema
		StructType schema = DataTypes.createStructType(fields);
		
		// Create the DataFrame
		Dataset<Row> df = spark.createDataFrame(inMemory, schema);
		
		// Work with the DataFrame
		Dataset<Row> result = df.groupBy("name").avg("age");
		
		// Show the result
		result.show();
							
		spark.close();				
	}
}
