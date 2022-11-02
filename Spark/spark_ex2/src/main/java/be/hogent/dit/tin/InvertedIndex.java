package be.hogent.dit.tin;

import org.apache.spark.sql.SparkSession;

public class InvertedIndex {
	
	SparkSession spark = SparkSession.builder()
			.appName("InvertedIndex")
			.master("local[*]")
			.getOrCreate();

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
