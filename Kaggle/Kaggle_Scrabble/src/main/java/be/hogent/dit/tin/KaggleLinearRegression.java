package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KaggleLinearRegression {

	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();

	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).csv("src/main/resources/games.csv");
	}

	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		return dataset.drop(col("game_id")).drop(col("first")).drop(col("time_control_name"))
				.drop(col("game_end_reason")).drop(col("created_at")).drop(col("lexicon")).drop(col("rating_mode"));
	}

	private Dataset<Row> changeTypeColumns(Dataset<Row> dataset) {
		return dataset.withColumn("initial_time_seconds", dataset.col("initial_time_seconds").cast("Integer"))
				.withColumn("increment_seconds", dataset.col("increment_seconds").cast("Integer"))
				.withColumn("max_overtime_minutes", dataset.col("max_overtime_minutes").cast("Integer"))
				.withColumn("game_duration_seconds", dataset.col("game_duration_seconds").cast("Integer"));
	}

	private Dataset<Row> stringIndexing(Dataset<Row> dataset) {
		StringIndexer indexer = new StringIndexer().setInputCol("winner").setOutputCol("ind_winner");

		return indexer.fit(dataset).transform(dataset);
	}

	private Dataset<Row> assemblingFeatures(Dataset<Row> dataset) {
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "initial_time_seconds",
				"increment_seconds", "max_overtime_minutes", "game_duration_seconds", "ind_winner" })
				.setOutputCol("features");

		return assembler.transform(dataset);
	}

	private Dataset<Row> minmaxScaling(Dataset<Row> dataset) {
		MinMaxScaler scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures");

		MinMaxScalerModel scalingModel = scaler.fit(dataset);

		return scalingModel.transform(dataset);
	}

	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] proportie) {
		Dataset<Row> final_data = dataset.select(col("features"), col("game_duration_seconds"));
		return final_data.randomSplit(proportie);
	}
	
	private LinearRegressionModel getLinearRegModel(Dataset<Row> dataset) {
		LinearRegression lr = new LinearRegression()
				.setFeaturesCol("features")
				.setLabelCol("game_duration_seconds");
		
		return lr.fit(dataset);
	}
	
	
	private void printSummaries(LinearRegressionSummary linregsum) {
		System.out.println("Trainingset --> RMSE: " + linregsum.rootMeanSquaredError());
		System.out.println("Testset --> RMSE: " + linregsum.rootMeanSquaredError());

		System.out.println("Trainingset --> r2: " + linregsum.r2());
		System.out.println("Testset --> r2: " + linregsum.r2());
	}
	
	private void printCorrelation(Dataset<Row> dataset) {
		Row r1 = Correlation.corr(dataset, "features").head();
		System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

		Row r2 = Correlation.corr(dataset, "features", "spearman").head();
		System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
	}
	
	
	

	public static void main(String[] args) {

		KaggleLinearRegression linreg = new KaggleLinearRegression();

		Dataset<Row> regression = linreg.getData();

		regression = linreg.dropColumns(regression);
		regression = linreg.changeTypeColumns(regression);
		regression = linreg.stringIndexing(regression);
		regression = regression.drop(col("winner"));
		regression = linreg.assemblingFeatures(regression);
		// regression = linreg.minmaxScaling(regression);

		double[] proportie = { 0.8, 0.2 };
		Dataset<Row>[] sets = linreg.splitSets(regression, proportie);
		
		LinearRegressionModel model = linreg.getLinearRegModel(sets[0]);

		LinearRegressionSummary testSummary = model.evaluate(sets[1]);

		System.out.println("Coefficients: " + model.coefficients() + " Intercept: " + model.intercept());

		LinearRegressionTrainingSummary trainingSummary = model.summary();
		
		/*
		 * 
		 * Analysing the ML data.
		 * 1. Show the residuals.
		 * 2. Print RMSE + R2 for the training & testset.
		 * 3. Print the correlation matrix for the dataset.
		 * 
		 */
		trainingSummary.residuals().show();
		linreg.printSummaries(trainingSummary);
		linreg.printCorrelation(regression);
		
		

	}

}
