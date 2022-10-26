package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
		Dataset<Row> tekstbestand = spark.read()
				.text("src/main/resources");
		
		// Toont een deel van wat is ingelezen.
		tekstbestand.show();
		spark.close();
	}
}
