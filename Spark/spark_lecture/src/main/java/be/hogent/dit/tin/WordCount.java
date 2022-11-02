package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;


import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.desc;


public class WordCount {

	public static void main(String[] args) {
	
		/*
		 * 
		 * 1. Spark session aanmaken met builder-object + op het einde 'spark.close()'
		 * 2. Data inlezen.
		 * 
		 */
		
		SparkSession spark = SparkSession.builder()
				.appName("WordCount")
				.master("local[*]")
				.getOrCreate();
		
		
		// Data uit de tekstbestanden inlezen.
		Dataset<Row> textDF = spark.read()
				.text("src/main/resources");
		
		// Toont een deel van wat is ingelezen.
		
		
		textDF = textDF
				.withColumn("words", explode(split(col("value"), " ")))
				.drop("value")
				.groupBy("words").count()
				.orderBy(desc("count"));
		
		textDF.show();
		
		spark.close();
	}
}
