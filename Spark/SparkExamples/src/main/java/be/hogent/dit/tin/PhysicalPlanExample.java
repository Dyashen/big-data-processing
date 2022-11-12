package be.hogent.dit.tin;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Simple class demonstrating the use of <code>explain</code> to see
 * the physical plan.
 * 
 * @author Stijn Lievens
 *
 */
public class PhysicalPlanExample {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
				.appName("Physical plan example")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> flightData2015 = spark.read()
				.option("header", true)
				.option("inferschema", true)				
				.csv("src/main/resources/2015-summary.csv");
		
		flightData2015.sort("count").explain();
		
		 List<Row> result = flightData2015.sort("count").takeAsList(3);
		 	 
		 // Set 5 output partitions for shuffling
		 spark.conf().set("spark.sql.shuffle.partitions", "5");
		 		 
		 flightData2015.groupBy("DEST_COUNTRY_NAME").count().explain();
		 
		 // A longer example
		 flightData2015
		 	.groupBy("DEST_COUNTRY_NAME")
		 	.sum("count")
		 	.withColumnRenamed("sum(count)", "destination_total")
		 	.sort(desc("destination_total"))
		 	.limit(5)
		 	.explain();
		
		spark.close();

	}

}
