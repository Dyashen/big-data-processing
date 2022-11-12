package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AirPolutionLinReg {
	
	SparkSession spark = SparkSession.builder()
			.appName("KaggleLinearRegression")
			.master("local[*]")
			.getOrCreate();
	
	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).csv("src/main/resources/GlobalAirPolution.csv");
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
		return dataset
				.withColumn("AQI Value", dataset.col("AQI Value").cast("Integer"))
				.withColumn("CO AQI Value", dataset.col("CO AQI Value").cast("Integer"))
				.withColumn("Ozone AQI Value", dataset.col("Ozone AQI Value").cast("Integer"))
				.withColumn("NO2 AQI Value", dataset.col("NO2 AQI Value").cast("Integer"))
				.withColumn("pm AQI Value", dataset.col("pm AQI Value").cast("Integer"));
	}
	
	
	private Dataset<Row> assemblingFeatures(Dataset<Row> dataset) {
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] {"CO AQI Value", "Ozone AQI Value", "NO2 AQI Value", "pm AQI Value" })
				.setOutputCol("features");

		return assembler.transform(dataset);
	}
	
	private Dataset<Row> standardScaling(Dataset<Row> dataset) {
		
		StandardScaler scaler = new StandardScaler()
				  .setInputCol("features")
				  .setOutputCol("scaledFeatures")
				  .setWithStd(true)
				  .setWithMean(false);

		// Compute summary statistics by fitting the StandardScaler
		StandardScalerModel scalerModel = scaler.fit(dataset);

		// Normalize each feature to have unit standard deviation.
		return scalerModel.transform(dataset);
		
	}
	
	private Dataset<Row> minmaxScaling(Dataset<Row> dataset) {
		MinMaxScaler scaler = new MinMaxScaler()
				.setInputCol("features")
				.setOutputCol("scaledFeatures");

		MinMaxScalerModel scalingModel = scaler.fit(dataset);

		return scalingModel.transform(dataset);
	}
	
	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		Dataset<Row> final_data = dataset.select(col("features"), col("AQI Value"));
		return final_data.randomSplit(verhouding);
	}
	
	private LinearRegressionModel getLinearRegModel(Dataset<Row> dataset) {
		LinearRegression lr = new LinearRegression()
				.setFeaturesCol("features")
				.setLabelCol("AQI Value");
		
		return lr.fit(dataset);
	}
	
	public static void main(String[] args) {
		
		AirPolutionLinReg aplinreg = new AirPolutionLinReg();
		Dataset<Row> regression = aplinreg.getData();
		
		regression = aplinreg.dropColumns(regression);
		regression = aplinreg.changeTypeColumns(regression);
		regression = aplinreg.assemblingFeatures(regression);
		
		//regression = aplinreg.standardScaling(regression);
		//regression = aplinreg.minmaxScaling(regression);
		
		double[] verhouding = { 0.8, 0.2 };
		Dataset<Row>[] sets = aplinreg.splitSets(regression, verhouding);
		
		
		LinearRegressionModel model = aplinreg.getLinearRegModel(sets[0]);

		LinearRegressionSummary testSummary = model.evaluate(sets[1]);

		System.out.println("Coefficients: " + model.coefficients() +
				" Intercept: " + model.intercept());

		LinearRegressionTrainingSummary trainingSummary = model.summary();
		
		//System.out.println("numIterations: " + trainingSummary.totalIterations());
		//System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));

		trainingSummary.residuals().show();

		System.out.println("Trainingset --> RMSE: " + trainingSummary.rootMeanSquaredError());
		
		System.out.println("Testset --> RMSE: " + testSummary.rootMeanSquaredError());

		System.out.println("Trainingset --> r2: " + trainingSummary.r2());
		
		System.out.println("Testset --> r2: " + testSummary.r2());

		
		
		

	}

}
