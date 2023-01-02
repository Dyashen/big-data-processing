package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KaggleRegression {

	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]")
			.config("spark.master", "local[*]").getOrCreate();

	private Dataset<Row> getTraining() {
		return this.spark.read().option("header", true).csv("src/main/resources/train.csv");
	}
	
	private Dataset<Row> getTest() {
		return this.spark.read().option("header", true).csv("src/main/resources/test.csv");
	}

	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		dataset = dataset.na().drop();
		return dataset
				.drop(col("game_id"));
	}

	/*
	 * De types van de kolommen staan op Strings. We werken hier met regressie, dus we moeten alles aanpassen naar integers.
	 */
	private Dataset<Row> changeTypeColumns(Dataset<Row> dataset) {
		return dataset
				.withColumn("score", dataset.col("score").cast("Integer"))
				.withColumn("rating", dataset.col("rating").cast("Integer"));
	}

	/*
	 * Correlatie tussen de verschillende features tonen.
	 */
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
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("rating")
					.setPredictionCol("prediction").setMetricName(metricType);

			double calc = evaluator.evaluate(predictions);
			
			System.out.printf("%s: \t%.5f \n", metricType, calc);
		}
	}

	public static void main(String[] args) {

		KaggleRegression kagglereg = new KaggleRegression();
		kagglereg.spark.sparkContext().setLogLevel("ERROR");

		/*
		 * Alle transformers en de evaluator, nodig voor de pipeline, opslaan onder een object.
		 */
		StringIndexer indexer = new StringIndexer()
				.setHandleInvalid("keep")
				.setInputCol("nickname")
				.setOutputCol("nickname_indexed");
		
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "nickname_indexed","score" })
				.setOutputCol("features");
		
		MinMaxScaler minmax = new MinMaxScaler()
				.setInputCol("features")
				.setOutputCol("scaledFeatures");
		
		RegressionEvaluator regEval = new RegressionEvaluator()
				.setLabelCol("rating")
				.setMetricName("rmse");


		/*
		 * Data ophalen
		 * De onnodige kolommen halen we uit het dataframe. 
		 * De types van de kolommen staan op string wat we moeten aanpassen naar integers.
		 */
		Dataset<Row> train = kagglereg.getTraining();
		Dataset<Row> test = kagglereg.getTest();
		
		train = kagglereg.dropColumns(train);
		train = kagglereg.changeTypeColumns(train);
		
		test = kagglereg.dropColumns(test);
		test = kagglereg.changeTypeColumns(test);
		
		/*
		 * Lineaire regressie
		 */
		System.out.printf("\n_-* Lineaire Regressie *-_\n");
		
		LinearRegression linreg = new LinearRegression()
				.setLabelCol("rating")
				.setFeaturesCol("scaledFeatures");

		Pipeline pipelineLinReg = new Pipeline()
				.setStages(new PipelineStage[] { indexer, assembler, minmax, linreg });

		PipelineModel model = pipelineLinReg.fit(train);
		Dataset<Row> predictions = model.transform(test);

		kagglereg.printCorrelation(predictions);
		kagglereg.printRegressionEvaluation(predictions);
		
		/*
		 * Random Forest Regression
		 */
		System.out.printf("\n_-* Random Forest Regression *-_\n");
		RandomForestRegressor rfr =  new RandomForestRegressor().setLabelCol("rating").setFeaturesCol("scaledFeatures");

		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, minmax, linreg });

		ParamMap[] paramGridRFR = new ParamGridBuilder()
				.addGrid(rfr.maxDepth(), new int[] {5, 10, 15, 20, 25, 30})
				.addGrid(rfr.numTrees(), new int[] {20, 40, 60, 80, 100, 120})
				.build();

		CrossValidator cvRFR = new CrossValidator().setEstimator(pipelineRFR).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridRFR);
		CrossValidatorModel cvmRFR = cvRFR.fit(train);
		
		Dataset<Row> cvPredictionsRFR = cvmRFR.transform(test);
		kagglereg.printRegressionEvaluation(cvPredictionsRFR);
		
		cvPredictionsRFR.show();

		kagglereg.spark.stop();
	}

}
