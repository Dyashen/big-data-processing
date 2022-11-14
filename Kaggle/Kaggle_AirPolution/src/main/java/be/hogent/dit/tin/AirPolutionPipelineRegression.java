package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AirPolutionPipelineRegression {

	/*
	 * SparkSession maken.
	 */
	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();

	/*
	 * Data ophalen. De CSV staat onder /src/main/resources
	 */
	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).csv("src/main/resources/GlobalAirPolution.csv");
	}

	/*
	 * De kolommen die we niet nodig hebben verwijderen we.
	 */
	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		return dataset.drop(col("Country")).drop(col("City")).drop(col("AQI Category")).drop(col("CO AQI Category"))
				.drop(col("Ozone AQI Category")).drop(col("NO2 AQI Category")).drop(col("`pm2.5 AQI Category`"));
	}

	
	/*
	 * De kolommen worden initieel opgeslaan als strings. Dit moet worden aangepast.
	 */
	private Dataset<Row> changeTypeColumns(Dataset<Row> dataset) {
		return dataset.withColumn("AQI Value", dataset.col("AQI Value").cast("Integer"))
				.withColumn("CO AQI Value", dataset.col("CO AQI Value").cast("Integer"))
				.withColumn("Ozone AQI Value", dataset.col("Ozone AQI Value").cast("Integer"))
				.withColumn("NO2 AQI Value", dataset.col("NO2 AQI Value").cast("Integer"))
				.withColumn("pm AQI Value", dataset.col("pm AQI Value").cast("Integer"));
	}

	/*
	 * De assembler gaat alle kolommen omzetten naar één kolom. Om een voorspelling
	 * te maken is een vector nodig van alle features.
	 */
	private VectorAssembler getAssembler() {
		return new VectorAssembler()
				.setInputCols(new String[] { "CO AQI Value", "Ozone AQI Value", "NO2 AQI Value", "pm AQI Value" })
				.setOutputCol("features");
	}

	/*
	 * Alternatief op de MinMaxScaler. Dit wordt niet gebruikt.
	 */
	private StandardScaler getStandardScaler() {
		return new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true)
				.setWithMean(false);
	}

	/*
	 * De features op een gelijke schaal brengen. Alles moet tussen 0 en 1 liggen.
	 */
	private MinMaxScaler getMinMaxScaler() {
		return new MinMaxScaler().setMax(1.0).setMin(0.0).setInputCol("features").setOutputCol("scaledFeatures");
	}

	/*
	 * Een dataset opsplitsen naargelang een verhouding. De verhouding moeten twee doubles zijn: %training & %test.
	 */
	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	
	private LinearRegression getLinearRegModel() {
		return new LinearRegression().setFeaturesCol("features").setLabelCol("AQI Value");
	}
	
	/*
	 * Object voor het Random Forest Regressie model.
	 */
	private RandomForestRegressor getRandomForestRegModel(int maxDepth) {
		return new RandomForestRegressor().setLabelCol("AQI Value").setFeaturesCol("features").setMaxDepth(maxDepth);
	}
	
	/*
	 * Vier metrieken worden bijgehouden in een array. 
	 * We hergebruiken het deel code om zo enkel de metricname aan te passen. 
	 * De evaluatie gebeurt op dezelfde label en voorspelde kolom.
	 */
	private void printRegressionEvaluation(Dataset<Row> predictions) {

		String[] metricTypes = { "mse", "rmse", "r2", "mae" };

		for (String metricType : metricTypes) {
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("AQI Value").setPredictionCol("prediction")
					.setMetricName(metricType);

			double calc = evaluator.evaluate(predictions);

			System.out.printf("%s: %.5f \n", metricType, calc);
		}
	}

	public static void main(String[] args) {

		AirPolutionPipelineRegression apreg = new AirPolutionPipelineRegression();
		apreg.spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> dataset = apreg.getData();
		

		// Voorbereiding
		dataset = apreg.dropColumns(dataset);
		dataset = apreg.changeTypeColumns(dataset);
		Dataset<Row>[] sets = apreg.splitSets(dataset, new double[] { 0.8, 0.2 });

		/*
		 * Lineair regressiemodel. Er wordt gewerkt met een pipeline: 1. De kolommen,
		 * dat we nodig hebben als feature, worden naar een vector van features omgezet.
		 * 2. Min-maxscaling. De features gaan een waarde hebben binnen het bereik (0
		 * --> 1). 3. Het lineaire regressiemodel.
		 */
		System.out.println("_-* Linear Regression *-_");
		Pipeline pipelineLinReg = new Pipeline().setStages(new PipelineStage[] { apreg.getAssembler(),
				// ff.getMinMaxScaler(),
				apreg.getLinearRegModel() });

		/*
		 * Lineair regressiemodel evalueren. De trainingset (80%) wordt gebruikt om het
		 * model te trainen. De testset (20%) wordt gebruikt om voorspellingen te maken.
		 * Hier wordt de burning area voorspelt met gegeven features. De
		 * evaluatiemetrieken (rmse, mse, mae en r2) worden uitgeprint.
		 */
		PipelineModel model = pipelineLinReg.fit(sets[0]);
		Dataset<Row> predictions = model.transform(sets[1]);
		apreg.printRegressionEvaluation(predictions);
		
		
		/*
		 * 
		 * Random Forest Regression De pipeline hier volgt dezelfde structuur mits de
		 * uitzondering van het randomforestmodel.
		 * 
		 */

		for (int i = 5; i < 30; i += 5) {
			System.out.printf("\n_-* Random Forest Regression %d *-_\n", i);
			Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { 
					apreg.getAssembler(),
					// ff.getMinMaxScaler(),
					apreg.getRandomForestRegModel(i) });

			PipelineModel modelRFR = pipelineRFR.fit(sets[0]);
			Dataset<Row> predictionsRFR = modelRFR.transform(sets[1]);
			apreg.printRegressionEvaluation(predictionsRFR);

		}
	}
}
