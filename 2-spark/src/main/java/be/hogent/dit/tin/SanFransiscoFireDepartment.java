package be.hogent.dit.tin;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class SanFransiscoFireDepartment {

	static SparkSession spark;
	static final String PATH = "src/main/resources/output";

	public static void main(String[] args) {

		spark = SparkSession.builder().appName("InvertedIndex").master("local[*]").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		List<StructField> fields = Arrays.asList(DataTypes.createStructField("CallNumber", DataTypes.IntegerType, true),
				DataTypes.createStructField("UnitID", DataTypes.StringType, true),
				DataTypes.createStructField("IncidentNumber", DataTypes.IntegerType, true),
				DataTypes.createStructField("CallType", DataTypes.StringType, true),
				DataTypes.createStructField("CallDate", DataTypes.StringType, true),
				DataTypes.createStructField("WatchDate", DataTypes.StringType, true),
				DataTypes.createStructField("CallFinalDisposition", DataTypes.StringType, true),
				DataTypes.createStructField("AvailableDtTm", DataTypes.StringType, true),
				DataTypes.createStructField("Address", DataTypes.StringType, true),
				DataTypes.createStructField("City", DataTypes.StringType, true),
				DataTypes.createStructField("Zipcode", DataTypes.IntegerType, true),
				DataTypes.createStructField("Battalion", DataTypes.StringType, true),
				DataTypes.createStructField("StationArea", DataTypes.StringType, true),
				DataTypes.createStructField("Box", DataTypes.StringType, true),
				DataTypes.createStructField("OriginalPriority", DataTypes.StringType, true),
				DataTypes.createStructField("Priority", DataTypes.StringType, true),
				DataTypes.createStructField("FinalPriority", DataTypes.IntegerType, true),
				DataTypes.createStructField("ALSUnit", DataTypes.StringType, true),
				DataTypes.createStructField("CallTypeGroup", DataTypes.StringType, true),
				DataTypes.createStructField("NumAlarms", DataTypes.IntegerType, true),
				DataTypes.createStructField("UnitType", DataTypes.StringType, true),
				DataTypes.createStructField("UnitSequenceInCallDispatch", DataTypes.IntegerType, true),
				DataTypes.createStructField("FirePreventionDistrict", DataTypes.StringType, true),
				DataTypes.createStructField("SupervisorDistrict", DataTypes.StringType, true),
				DataTypes.createStructField("Neighborhood", DataTypes.StringType, true),
				DataTypes.createStructField("Location", DataTypes.StringType, true),
				DataTypes.createStructField("RowID", DataTypes.StringType, true),
				DataTypes.createStructField("Delay", DataTypes.DoubleType, true));
		
		StructType schema = DataTypes.createStructType(fields);

		// Data inlezen
		Dataset<Row> department = spark.read()
						.option("header", true)
						.schema(schema)
						.csv("src/main/resources/sf-fire-calls.csv");
		
		// Part I
		
		// convert the columns calldate and watchdate
		department = department
				.withColumn("Calldate", to_date(col("CallDate"), "MM/dd/yyyy"))
				.withColumn("Watchdate", to_date(col("Watchdate"), "MM/dd/yyyy"));
		
		// convert the column availabledtm to a timestamp
		department = department
				.withColumn("AvailableDtTm", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"));
		
		// save the dataframe as a parquet file
		department.write()
				.mode(SaveMode.Overwrite)
				.parquet(PATH);
		
		
		// Part II
		
		// Alle verschillende calltypes in 2018 met WHERE
		department
			.where(year(col("Calldate")).equalTo("2018"))
			.groupBy(col("CallType"))
			.agg(count("*").as("Aantal"))
			.select("CallType")
			.orderBy("CallType")
			.show(false);
		
		// Alle verschillende calltypes in 2018 met filter
		department
			.filter(year(col("Calldate")).equalTo("2018"))
			.groupBy(col("CallType"))
			.agg(count("*").as("Aantal"))
			.select("CallType")
			.orderBy("CallType")
			.show(false);
		
		// Alle maanden in 2018 met het hoogst aantal calls.
		department
			.filter(year(col("Calldate")).equalTo("2018"))
			.groupBy(month(col("Calldate")).as("Maandnummer"))
			.agg(count("*").as("Aantal"))
			.orderBy(desc("Aantal"))
			.show(false);
		
		// De neighborhood met de meeste oproepen in 2018
		department
			.filter(year(col("Calldate")).equalTo("2018"))
			.groupBy("neighborhood")
			.agg(count("*").as("Aantal"))
			.orderBy(desc("Aantal"))
			.limit(1)
			.show(false);
		
		// De neighborhoods met de slechtste gemiddelde vertraagtijden bij een omroep.
		department
			.filter(year(col("Calldate")).equalTo("2018"))
			.groupBy("neighborhood")
			.agg(avg(col("Delay")).as("avgResponse"))
			.orderBy(desc("avgResponse"))
			.select(col("neighborhood"), bround(col("avgResponse"), 2))
			.show(false);
	
		// De week in 2018 met de meeste calls
		department
			.filter(year(col("Calldate")).equalTo("2018"))
			.groupBy(weekofyear(col("Calldate")).as("Weeknummer"))
			.agg(count("*").as("Aantal"))
			.limit(1)
			.show(false);
		
		spark.close();
	}
}
