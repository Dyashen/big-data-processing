package be.hogent.dit.tin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple class showing how to use JOINs in Spark.
 * 
 * @author Stijn Lievens
 *
 */
public class JoinExample {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
				.appName("Join example")
				.master("local[*]")
				.getOrCreate();
		
		// Create DataFrame of Person
		List<StructField> personFields = Arrays.asList(
				DataTypes.createStructField("id", DataTypes.LongType, false),
				DataTypes.createStructField("name", DataTypes.StringType, false),
				DataTypes.createStructField("graduate_program", DataTypes.IntegerType, false),
				DataTypes.createStructField("spark_status", DataTypes.createArrayType(DataTypes.IntegerType), false)
				);
		
		StructType personSchema = DataTypes.createStructType(personFields);
		
		List<Row> personData = new ArrayList<>();
		
		personData.add(RowFactory.create(0L, "Bill Chambers", 0, new int [] {100}));		
		personData.add(RowFactory.create(1L, "Matei Zaharia", 1, new int [] {500,250,100}));
		personData.add(RowFactory.create(2L, "Michael Ambrust", 1, new int [] {250,100}));
		
		Dataset<Row> personDF = spark.createDataFrame(personData, personSchema);
		
		personDF.show();
		
		// Create DataFrame of Graduate Program
		List<StructField> programFields = Arrays.asList(
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("degree", DataTypes.StringType, false),
				DataTypes.createStructField("department", DataTypes.StringType, false),
				DataTypes.createStructField("school", DataTypes.StringType, false)
				);
		
		StructType programSchema = DataTypes.createStructType(programFields);
		
		List<Row> programData = new ArrayList<>();
		
		programData.add(RowFactory.create(0, "Masters", "School of Information", "UC Berkeley"));
		programData.add(RowFactory.create(2, "Masters", "EECS", "UC Berkeley"));
		programData.add(RowFactory.create(1, "Ph.D.", "EECS", "UC Berkeley"));
		
		Dataset<Row> graduateProgramDF = spark.createDataFrame(programData, programSchema);
		
		graduateProgramDF.show();
		
		// Create SparkStatus DataFrame
		List<StructField> statusFields = Arrays.asList(
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("status", DataTypes.StringType, false)				
				);
		
		
		StructType statusSchema = DataTypes.createStructType(statusFields);
		
		List<Row> statusData = new ArrayList<>();
		
		statusData.add(RowFactory.create(500, "Vice President"));
		statusData.add(RowFactory.create(250, "PMC Member"));
		statusData.add(RowFactory.create(100, "Contributor"));
		
		Dataset<Row> statusDF = spark.createDataFrame(statusData, statusSchema);
		
		statusDF.show();
		
		// Inner join personDF and graduateProgramDF on 'graduate_program' and 'id'
		Column joinExpression = personDF.col("graduate_program").equalTo(graduateProgramDF.col("id"));
		Dataset<Row> joined = personDF.join(graduateProgramDF, joinExpression);
		System.out.println("Inner join of personDF and graduateProgramDF on 'natural' key");
		joined.show();
		
		// Outer join of the same two DataFrames
		Dataset<Row> joinedOuter = personDF.join(graduateProgramDF, joinExpression, "outer");
		System.out.println("Outer join of personDF and graduateProgramDF on 'natural' key");
		joinedOuter.show();
			
		spark.close();

	}

}
