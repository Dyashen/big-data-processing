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
 * Small class to demonstrate the use of CSV and Parquet files.
 * 
 * Read a CSV-file and write it back into Parquet format. 
 * Read the Parquet file to check that the number of rows is identical.
 * 
 * @author Stijn Lievens
 */
public class DataFrameWriteExample {

	public static void main(String[] args) {			
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		System.setProperty("hadoop.home.dir", "C:\\tmp\\winutils-extra\\hadoop");
		
		
		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("DataFrameWriteExample").master("local[*]")
				.getOrCreate();
		
		List<StructField> fields = Arrays.asList(
		    DataTypes.createStructField("student_id", DataTypes.IntegerType, false),
		    DataTypes.createStructField("exam_center_id",  DataTypes.IntegerType, false),
		    DataTypes.createStructField("subject",  DataTypes.StringType, false),
		    DataTypes.createStructField("year",  DataTypes.IntegerType, false),
		    DataTypes.createStructField("quarter",  DataTypes.IntegerType, false),
		    DataTypes.createStructField("score",  DataTypes.IntegerType, false),
		    DataTypes.createStructField("grade",  DataTypes.StringType, false)
			);
		
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> df = spark.read()
				.option("header", true)
				.schema(schema)				
				.csv("src/main/resources/students.csv");
		
		System.out.println("Number of rows: " + df.count());
		
		String path = "file:///C:/tmp/students";
		df.write().format("parquet")
			.mode("overwrite")
			.save(path);
				
		/* No need to specify schema */
		Dataset<Row> dfParquet = spark.read().parquet(path);
		
		System.out.println("Number of rows after saving and reading: " + dfParquet.count());
					
		spark.close();
	}

}
