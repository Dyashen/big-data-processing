package be.hogent.dit.tin;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.weekofyear;;

public class FireDepartment {

	SparkSession spark = SparkSession.builder().appName("InvertedIndex").master("local[*]").getOrCreate();

	private Dataset<Row> getFile(String pathname_data) {

		// Data uit de tekstbestanden inlezen.
		return null;
	}

	public static void main(String[] args) {

		FireDepartment fd = new FireDepartment();
		fd.spark.sparkContext().setLogLevel("ERROR");

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

		Dataset<Row> dataset = fd.spark.read().option("header", true).schema(schema)
				.csv("src/main/resources/sf-fire-calls.csv");

		dataset = dataset.withColumn("CallDate_", to_date(col("CallDate"), "MM/dd/yyyy"))
				.withColumn("WatchDate_", to_date(col("WatchDate"), "MM/dd/yyyy"))
				.withColumn("AvailableDtTm_", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
				.drop(col("CallDate")).drop(col("WatchDate")).drop(col("AvailableDtTm"));
		
		dataset.write().parquet("src/main/resources/output.parquet");

		/*
		 * What where all the different types of calls in 2018?
		 */
		dataset.filter(year(col("CallDate_")).geq("2018")).groupBy("CallType").count().select("CallType").show(5);

		/*
		 * What months within 2018 saw the highest number of calls?
		 */
		dataset.filter(year(col("CallDate_")).geq("2018")).groupBy(month(col("CallDate_"))).count()
				.orderBy(col("count").desc()).show(5);

		/*
		 * Which neighborhood in San Fransisco generated the most calls in 2018?
		 */
		dataset.filter(year(col("CallDate_")).geq("2018")).groupBy(col("Neighborhood")).count()
				.orderBy(col("count").desc()).show(5);

		/*
		 * Which neighborhoods had the worst response times in 2018 (on average). Round
		 * the average response times to two decimal places.
		 */
		dataset.filter(year(col("CallDate_")).geq("2018")).groupBy(col("Neighborhood"))
				.agg(avg(col("Delay")).as("AVG_Response")).orderBy(col("AVG_Response").desc()).show(5);
		
		/*
		 * Which week in 2018 had the most calls?
		 */
		dataset.filter(year(col("CallDate_")).geq("2018")).groupBy(weekofyear(col("CallDate_"))).count().orderBy(col("count").desc()).show(5);

	}
}