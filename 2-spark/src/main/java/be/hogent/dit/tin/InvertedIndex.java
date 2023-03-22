package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class InvertedIndex {
	
	static SparkSession spark;
	
	static final String INPUT_PATH = "src/main/resources/*";
	
	static UDF1<String, String> parseFilename = new UDF1<String, String>()
	{
		public String call(String fullPath) throws Exception {
			int index = fullPath.lastIndexOf("/");
			return fullPath.substring(index + 1, fullPath.length());
		};
	};

	public static void main(String[] args) {

		spark = SparkSession.builder().appName(InvertedIndex.class.getName()).master("local[1]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> invertedIndex = spark.read()
									.text(INPUT_PATH);
		
		spark.udf().register("parseFilename", parseFilename, DataTypes.StringType);
		
		invertedIndex
				.withColumn("raw", input_file_name().cast("String")) 			// pad ophalen
				.withColumn("filename", call_udf("parseFilename", col("raw"))) 	// filename parsen
				.withColumn("woorden", explode(split(col("value"), " ")))		// woorden eruit halen
				.drop("raw")													// niet meer nodig
				.groupBy(col("woorden"), col("filename"))						// groeperen per woord & 
				.agg(count("woorden").as("aantalVoorkomens"))
				.orderBy(desc("aantalVoorkomens"))
				.show();
		
		
		// extra aanvulling: ervoor zorgen dat de bestanden waarin spark terechtkomt opnemen
		invertedIndex
			.withColumn("raw", input_file_name().cast("String")) 			// pad ophalen
			.withColumn("filename", call_udf("parseFilename", col("raw"))) 	// filename parsen
			.withColumn("woorden", explode(split(col("value"), " ")))  		// woorden eruit halen
			.withColumn("woorden", lower(col("woorden")))
			.drop("raw")													// niet meer nodig
			.groupBy(col("woorden"))										// groeperen per woord & 
			.agg(count("woorden").as("aantalVoorkomens"), 					// aantal voorkomens
					collect_set(col("filename")).as("bestandenVoorkomens"))	// bestanden waar woorden in voorkomen
			.orderBy(desc("aantalVoorkomens"))
			.show(false);	
	}
}
