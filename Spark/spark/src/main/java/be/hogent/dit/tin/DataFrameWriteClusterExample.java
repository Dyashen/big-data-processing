package be.hogent.dit.tin;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Read a CSV-file and write it back into Parquet format. 
 * Read the Parquet file to check that the number of rows is identical.
 * This example is meant to run a cluster where "hdfs://namenode:9000/" is available.
 * 
 * @author Stijn Lievens
 */
public class DataFrameWriteClusterExample {
	
	private static final String HDFS_URL = "hdfs://namenode:9000";
	private static final String CSV_URL = "/example/students.csv";
	private static final String PARQUET_DIR = "/example/parquet/students";

	public static void main(String[] args) {
		System.out.print("Java Specification Version: ");
	    System.out.println(System.getProperty("java.specification.version"));
	    System.out.print("java Runtime Environment (JRE) version: ");
	    System.out.println(System.getProperty("java.version"));
				
		// Create Spark Session
		SparkSession spark = SparkSession.builder()
				.appName("DataFrameWriteExample")
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
				.csv(HDFS_URL + CSV_URL);
		
		System.out.println("Number of rows: " + df.count());
		
		df.write().format("parquet")
			.mode("overwrite")
			.save(HDFS_URL + PARQUET_DIR);
				
		/* No need to specify schema */
		Dataset<Row> dfParquet = spark.read().parquet(HDFS_URL + PARQUET_DIR);
		
		System.out.println("Number of rows after saving and reading: " + dfParquet.count());
					
		spark.close();
	}

}
