package be.hogent.dit.tin;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class KaggleLinearRegression {
	
	SparkSession spark = SparkSession.builder()
			.appName("KaggleLinearRegression")
			.master("local[*]")
			.getOrCreate();
	
	
	private Dataset<Row> readData(String pathname_data){
		
		return this.spark
				.read()
				.format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
				.option("header", true)
				.load("src/main/resources/games.csv");
	}
	
	private LinearRegression createModel() {
		return new LinearRegression()
				.setMaxIter(10)
				.setRegParam(0.3)
				.setElasticNetParam(0.8);
	}
	

	public static void main(String[] args) {
		
		KaggleLinearRegression linreg = new KaggleLinearRegression();
		
		Dataset<Row> data = linreg.readData("src/main/resources/games.csv");
		
		data.show();
		
		

		StringIndexerModel labelIndexer = new StringIndexer()
			    .setInputCol("label")
			    .setOutputCol("indexedLabel")
			    .fit(data);
		
		MinMaxScalerModel featureScaler = new MinMaxScaler()
			    .setInputCol("features")
			    .setOutputCol("scaledFeatures")
			    .fit(data);
	
	}

}
