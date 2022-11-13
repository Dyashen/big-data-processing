package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AirPolutionRFR {
	
	SparkSession spark = SparkSession
			.builder()
			.appName("KaggleLinearRegression")
			.master("local[*]")
			.getOrCreate();
	
	private Dataset<Row> getData() {
		return this.spark
				.read()
				.option("header", true)
				.csv("src/main/resources/GlobalAirPolution.csv");
	}
	
	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		return dataset
				.drop(col("Country"))
				.drop(col("City"))
				.drop(col("AQI Category"))
				.drop(col("CO AQI Category"))
				.drop(col("Ozone AQI Category"))
				.drop(col("NO2 AQI Category"))
				.drop(col("`pm2.5 AQI Category`"));
	}

	private Dataset<Row> changeTypeColumns(Dataset<Row> dataset) {
		return dataset.withColumn("AQI Value", dataset.col("AQI Value").cast("Integer"))
				.withColumn("CO AQI Value", dataset.col("CO AQI Value").cast("Integer"))
				.withColumn("Ozone AQI Value", dataset.col("Ozone AQI Value").cast("Integer"))
				.withColumn("NO2 AQI Value", dataset.col("NO2 AQI Value").cast("Integer"))
				.withColumn("pm AQI Value", dataset.col("pm AQI Value").cast("Integer"));
	}
	
	private VectorAssembler getAssembler() {
		return new VectorAssembler()
				.setInputCols(new String[] { "CO AQI Value", "Ozone AQI Value", "NO2 AQI Value", "pm AQI Value" })
				.setOutputCol("features");
	}
	
	private MinMaxScaler getMinMaxScaler() {
		return new MinMaxScaler()
				.setInputCol("features")
				.setOutputCol("scaledFeatures");
	}
	
	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}
	
	private RandomForestRegressor getRFR() {
		return new RandomForestRegressor()
				.setFeaturesCol("features")
				.setLabelCol("AQI Value");
	}

	public static void main(String[] args) {
		
		AirPolutionRFR aprfr = new AirPolutionRFR();
		Dataset<Row> dataset = aprfr.getData();
		
		dataset = aprfr.dropColumns(dataset);
		dataset = aprfr.changeTypeColumns(dataset);
		
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {
				aprfr.getAssembler(),
				//aplinreg.getMinMaxScaler(),
				aprfr.getRFR()
				});
		
		/* Dataframe opsplitsen in twee delen. */
		double[] verhouding = {0.8,0.2};
		Dataset<Row>[] sets = aprfr.splitSets(dataset, verhouding);
		
		
		/* Model trainen met de 80%. */
		PipelineModel model = pipeline.fit(sets[0]);
		
		Dataset<Row> predictions = model.transform(sets[1]);
		
		predictions.select("prediction", "AQI Value", "features").show(5);
		
		RegressionEvaluator evaluator = new RegressionEvaluator()
				  .setLabelCol("AQI Value")
				  .setPredictionCol("prediction")
				  .setMetricName("rmse");
		double rmse = evaluator.evaluate(predictions);
		System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

	}

}
