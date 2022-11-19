package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KaggleRegression {

	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]")
			.config("spark.master", "local[*]").getOrCreate();

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

	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] proportie) {
		return dataset.randomSplit(proportie);
	}

	private void printCorrelation(Dataset<Row> dataset) {
		Row r1 = Correlation.corr(dataset, "features").head();

		System.out.printf("\n\nCorrelation Matrix\n");
		Matrix matrix = r1.getAs(0);
		for (int i = 0; i < matrix.numRows(); i++) {
			for (int j = 0; j < matrix.numCols(); j++) {
				System.out.printf("%.2f \t", matrix.apply(i, j));
			}
			System.out.println();
		}
	}

	private void printRegressionEvaluation(Dataset<Row> predictions) {
		String[] metricTypes = { "mse", "rmse", "r2", "mae" };

		System.out.printf("\n\nMetrics:\n");

		for (String metricType : metricTypes) {
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("game_duration_seconds")
					.setPredictionCol("prediction").setMetricName(metricType);

			double calc = evaluator.evaluate(predictions);

			System.out.printf("%s: \t%.5f \n", metricType, calc);
		}
	}

	public static void main(String[] args) {

		KaggleRegression kagglereg = new KaggleRegression();

		kagglereg.spark.sparkContext().setLogLevel("ERROR");

		StringIndexer indexer = new StringIndexer().setInputCol("winner").setOutputCol("ind_winner");
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "initial_time_seconds",
				"increment_seconds", "max_overtime_minutes", "ind_winner" })
				.setOutputCol("features");
		MinMaxScaler minmax = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures");
		RegressionEvaluator regEval = new RegressionEvaluator().setLabelCol("game_duration_seconds").setMetricName("rmse");
		double[] verhouding = { 0.7, 0.3 };

		Dataset<Row> regressiondata = kagglereg.getData();

		regressiondata = kagglereg.dropColumns(regressiondata);
		regressiondata = kagglereg.changeTypeColumns(regressiondata);
		
		/*
		 * 
		 */
		System.out.printf("\n_-* Lineaire Regressie *-_\n");
		
		LinearRegression linreg = new LinearRegression().setLabelCol("game_duration_seconds")
				.setFeaturesCol("scaledFeatures");

		Pipeline pipelineLinReg = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, minmax, linreg });

		Dataset<Row>[] sets = kagglereg.splitSets(regressiondata, verhouding);
		PipelineModel model = pipelineLinReg.fit(sets[0]);
		Dataset<Row> predictions = model.transform(sets[1]);

		kagglereg.printCorrelation(predictions);
		kagglereg.printRegressionEvaluation(predictions);
		
		predictions.show();

		/*
		 * Random Forest Regression
		 */
		System.out.printf("\n_-* Random Forest Regression *-_\n");
		RandomForestRegressor rfr =  new RandomForestRegressor().setLabelCol("game_duration_seconds").setFeaturesCol("scaledFeatures");

		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, minmax, linreg });

		ParamMap[] paramGridRFR = new ParamGridBuilder()
				.addGrid(rfr.maxDepth(), new int[] {5, 10, 15, 20, 25, 30})
				.addGrid(rfr.numTrees(), new int[] {20, 40, 60, 80, 100, 120})
				.build();

		CrossValidator cvRFR = new CrossValidator().setEstimator(pipelineRFR).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridRFR);
		CrossValidatorModel cvmRFR = cvRFR.fit(sets[0]);

		System.out.printf("Beste model: %s\n", cvmRFR.bestModel().params().toString());
		Dataset<Row> cvPredictionsRFR = cvmRFR.transform(sets[1]);
		kagglereg.printRegressionEvaluation(cvPredictionsRFR);
		
		cvPredictionsRFR.show();

		// kagglereg.spark.stop();
	}

}
