package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.collect_list;


public class InvertedIndex {

	SparkSession spark = SparkSession.builder().appName("InvertedIndex").master("local[*]").getOrCreate();
	
	/*
	 * Tenzij je het in de main plaatst, altijd static voor de UDF schrijven!! Zo niet krijg je een serializable error.
	 */
	static UDF1<String, String> getFilename = new UDF1<String, String>() {
		public String call(String fullPath) throws Exception {
			int lastIndex = fullPath.lastIndexOf("/");
			return fullPath.substring(lastIndex + 1, fullPath.length());
		}
	};

	private Dataset<Row> getFiles(String pathname_data) {

		// Data uit de tekstbestanden inlezen.
		return spark.read().text(pathname_data).withColumn("raw", input_file_name().cast("String"))
				.withColumn("words", explode(split(col("value"), " "))).drop("value");
	}
	
	private Dataset<Row> addFilenameColumn(Dataset<Row> dataset) {
		
		this.spark.udf().register("getFilename", getFilename, DataTypes.StringType);
		
		return dataset.withColumn("filename", callUDF("getFilename", col("raw")))
				.drop("raw")
				.groupBy(col("words"), col("filename")).count()
				//.agg(collect_list("filename"))
				.orderBy("count");
	}
	
	private void writeCSV(Dataset<Row> dataset){
		dataset.write().option("header",true)
		   .csv("/tmp/spark_output/datacsv");
	}

	public static void main(String[] args) {

		InvertedIndex ii = new InvertedIndex();
		ii.spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> data = ii.getFiles("src/main/resources/*");
		data = ii.addFilenameColumn(data);
		
		data.show();
		
		//ii.writeCSV(data);

	}

}
