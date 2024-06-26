package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ForestFiresRegression {

	/*
	 * SparkSession maken.
	 */
	SparkSession spark = SparkSession.builder().appName("ForestFires").master("local[*]").getOrCreate();

	/*
	 * Data ophalen. De CSV staat onder /src/main/resources
	 */
	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).csv("src/main/resources/forestfires.csv");
	}

	/*
	 * De kolommen die we niet nodig hebben verwijderen we.
	 */
	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		return dataset.drop(col("X")).drop(col("Y")).drop(col("month")).drop(col("day"));
	}

	/*
	 * De kolommen worden initieel opgeslaan als strings. Dit moet worden aangepast.
	 */
	private Dataset<Row> changeTypeColumns(Dataset<Row> dataset) {
		return dataset.withColumn("FFMC", dataset.col("FFMC").cast("double"))
				.withColumn("DMC", dataset.col("DMC").cast("double")).withColumn("DC", dataset.col("DC").cast("double"))
				.withColumn("ISI", dataset.col("ISI").cast("double"))
				.withColumn("temp", dataset.col("temp").cast("double"))
				.withColumn("RH", dataset.col("RH").cast("double"))
				.withColumn("wind", dataset.col("wind").cast("double"))
				.withColumn("rain", dataset.col("rain").cast("double"))
				.withColumn("area", dataset.col("area").cast("double"));
	}

	/*
	 * Een dataset opsplitsen naargelang een verhouding. De verhouding moeten twee
	 * doubles zijn: %training & %test.
	 */
	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	/*
	 * Vier metrieken worden bijgehouden in een array. We hergebruiken het deel code
	 * om zo enkel de metricname aan te passen. De evaluatie gebeurt op dezelfde
	 * label en voorspelde kolom.
	 */
	private void printRegressionEvaluation(Dataset<Row> predictions) {

		String[] metricTypes = { "mse", "rmse", "r2", "mae" };

		for (String metricType : metricTypes) {
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("area").setPredictionCol("prediction")
					.setMetricName(metricType);

			double calc = evaluator.evaluate(predictions);

			System.out.printf("%s: \t%.5f \n", metricType, calc);
		}
	}

	public static void main(String[] args) {
		/*
		 * Het object hebben we nodig om de functies aan te spreken. We schakelen de
		 * "onnodige" tekst uit. Enkel foutmeldingen en de geprinte uitvoer zal in de
		 * terminal terechtkomen.
		 */
		ForestFiresRegression ff = new ForestFiresRegression();
		ff.spark.sparkContext().setLogLevel("ERROR");

		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "FFMC", "DMC", "DC", "ISI", "temp", "RH", "wind", "rain" })
				.setOutputCol("features");
		MinMaxScaler minmax = new MinMaxScaler().setMax(1.0).setMin(0.0).setInputCol("features")
				.setOutputCol("scaledFeatures");
		RegressionEvaluator regEval = new RegressionEvaluator().setLabelCol("area").setMetricName("rmse");

		/*
		 * Dataset ophalen + Datacleaning + Datasets splitsen.
		 */
		Dataset<Row> dataset = ff.getData();
		dataset = ff.dropColumns(dataset);
		dataset = ff.changeTypeColumns(dataset);
		Dataset<Row>[] sets = ff.splitSets(dataset, new double[] { 0.8, 0.2 });

		/*
		 * Lineair regressiemodel. Er wordt gewerkt met een pipeline: 1. De kolommen,
		 * dat we nodig hebben als feature, worden naar een vector van features omgezet.
		 * 2. Min-maxscaling. De features gaan een waarde hebben binnen het bereik (0
		 * --> 1). 3. Het lineaire regressiemodel.
		 */
		System.out.println("_-* Linear Regression *-_");

		LinearRegression linreg = new LinearRegression().setFeaturesCol("scaledFeatures").setLabelCol("area");

		Pipeline pipelineLinReg = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, linreg });

		/*
		 * Lineair regressiemodel evalueren. De trainingset (80%) wordt gebruikt om het
		 * model te trainen. De testset (20%) wordt gebruikt om voorspellingen te maken.
		 * Hier wordt de burning area voorspelt met gegeven features. De
		 * evaluatiemetrieken (rmse, mse, mae en r2) worden uitgeprint.
		 */
		PipelineModel model = pipelineLinReg.fit(sets[0]);
		Dataset<Row> predictions = model.transform(sets[1]);
		ff.printRegressionEvaluation(predictions);

		/*
		 * Random Forest Regression De pipeline hier volgt dezelfde structuur mits de
		 * uitzondering van het randomforestmodel.
		 */
		System.out.printf("\n_-* Random Forest Regression *-_\n");

		RandomForestRegressor rfr = new RandomForestRegressor().setLabelCol("area").setFeaturesCol("scaledFeatures");
		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, rfr });
		
		ParamMap[] paramGridRFR = new ParamGridBuilder()
				.addGrid(rfr.maxDepth(), new int[] {5, 10, 15, 20, 25, 30})
				.addGrid(rfr.numTrees(), new int[] {20, 40, 60, 80, 100, 120})
				.build();

		CrossValidator cvRFR = new CrossValidator().setEstimator(pipelineRFR).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridRFR);
		CrossValidatorModel cvmRFR = cvRFR.fit(sets[0]);

		System.out.printf("Beste model: %s\n", cvmRFR.bestModel().params().toString());
		Dataset<Row> cvPredictionsRFR = cvmRFR.transform(sets[1]);
		ff.printRegressionEvaluation(cvPredictionsRFR);

	}
}
