package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions.input_file_name;

public class InvertedIndex {
	
	SparkSession spark = SparkSession.builder()
			.appName("InvertedIndex")
			.master("local[*]")
			.getOrCreate();

	public Dataset<Row> getFiles(String pathname_data){
		
		spark.udf()
				  .register("get_only_file_name", (String fullPath) -> {
					     int lastIndex = fullPath.lastIndexOf("/");
					     return fullPath.substring(lastIndex, fullPath.length() - 1);
					    }, DataTypes.StringType);
		
		// Data uit de tekstbestanden inlezen.
		return spark
				.read()
				.text(pathname_data)
				.withColumn("filename", get_only_file_name(input_file_name()));
	}
	
	
	public static void main(String[] args) {

	InvertedIndex ii = new InvertedIndex();
	
	Dataset<Row> data = ii.getFiles("src/main/resources/*");
	
	data.show();
	
	}

}
