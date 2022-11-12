package be.hogent.dit.tin;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import static org.apache.spark.sql.functions.col;

public class KaggleLinearRegression {

	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();
	

	private Dataset<Row> readData(String pathname_data) {
		return this.spark.read().format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
				.option("header", true).load("src/main/resources/games.csv");
	}

	public static void main(String[] args) {

		KaggleLinearRegression linreg = new KaggleLinearRegression();

		Dataset<Row> ml_data = linreg.spark.read().option("header", true).csv("src/main/resources/games.csv"); 
		
		Dataset<Row> regression = ml_data;
		
		regression = regression
				.drop(col("game_id"))
				.drop(col("first"))
				.drop(col("time_control_name"))
				.drop(col("game_end_reason"))
				.drop(col("created_at"))
				.drop(col("lexicon"))
				.drop(col("rating_mode"));
		
		regression = regression.withColumn("initial_time_seconds", regression.col("initial_time_seconds").cast("Integer"));
		regression = regression.withColumn("increment_seconds", regression.col("increment_seconds").cast("Integer"));
		regression = regression.withColumn("max_overtime_minutes", regression.col("max_overtime_minutes").cast("Integer"));
		regression = regression.withColumn("game_duration_seconds", regression.col("game_duration_seconds").cast("Integer"));
		

		StringIndexer indexer = new StringIndexer()
				.setInputCol("winner")
				.setOutputCol("ind_winner");
		
		Dataset<Row> indexed_reg = indexer.fit(regression).transform(regression);
		
		indexed_reg = indexed_reg.drop(col("winner"));
		
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[]{"initial_time_seconds", "increment_seconds", "max_overtime_minutes", "game_duration_seconds", "ind_winner"})
				.setOutputCol("features");
		
		Dataset<Row> output = assembler.transform(indexed_reg);
		
//		MinMaxScaler scaler = new MinMaxScaler()
//				.setInputCol("features")
//				.setOutputCol("scaledFeatures");
//		
//		MinMaxScalerModel features_scaled = scaler.fit(output);
//		
//		Dataset<Row> scaled_df = features_scaled.transform(output);
		
		
		Dataset<Row> final_data = output.select(col("features"), col("game_duration_seconds"));
		
		double[] proportie = {0.8, 0.2};
		
		Dataset<Row>[] sets = final_data.randomSplit(proportie);
		
		sets[0].show();
		
		LinearRegression lr = new LinearRegression()
				.setFeaturesCol("features")
				.setLabelCol("game_duration_seconds");
		
		LinearRegressionModel model = lr.fit(sets[0]);
		
		LinearRegressionSummary testSummary = model.evaluate(sets[1]);
		
		
		
		System.out.println("Coefficients: "
		  + model.coefficients() + " Intercept: " + model.intercept());

		LinearRegressionTrainingSummary trainingSummary = model.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		
		trainingSummary.residuals().show();
		
		System.out.println("Trainingset --> RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("Testset --> RMSE: " + testSummary.rootMeanSquaredError());
		
		System.out.println("Trainingset --> r2: " + trainingSummary.r2());
		System.out.println("Testset --> r2: " + testSummary.r2());

	}

}
