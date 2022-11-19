package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AirPolutionPipelineRegression {

	/*
	 * SparkSession maken.
	 */
	SparkSession spark = SparkSession.builder().appName("AirPolutionRegression").master("local[*]").getOrCreate();

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
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("AQI Value")
					.setPredictionCol("prediction").setMetricName(metricType);

			double calc = evaluator.evaluate(predictions);

			System.out.printf("%s:\t%.5f \n", metricType, calc);
		}
	}

	public static void main(String[] args) {

		AirPolutionPipelineRegression apreg = new AirPolutionPipelineRegression();
		apreg.spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> dataset = apreg.getData();

		// Voorbereiding
		dataset = apreg.dropColumns(dataset);
		dataset = apreg.changeTypeColumns(dataset);
		Dataset<Row>[] sets = apreg.splitSets(dataset, new double[] { 0.7, 0.3 });

		MinMaxScaler minmax = apreg.getMinMaxScaler();
		VectorAssembler assembler = apreg.getAssembler();
		RegressionEvaluator regEval = new RegressionEvaluator().setLabelCol("AQI Value").setMetricName("rmse");

		/*
		 * Lineair regressiemodel. Er wordt gewerkt met een pipeline: 1. De kolommen,
		 * dat we nodig hebben als feature, worden naar een vector van features omgezet.
		 * 2. Min-maxscaling. De features gaan een waarde hebben binnen het bereik (0
		 * --> 1). 3. Het lineaire regressiemodel.
		 */
		
		
		System.out.printf("\n _-* Linear Regression *-_\n");
		
		LinearRegression linreg = new LinearRegression().setFeaturesCol("scaledFeatures").setLabelCol("AQI Value");
		
		Pipeline pipelineLinReg = new Pipeline()
				.setStages(new PipelineStage[] { assembler, minmax, linreg});

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
		 * uitzondering van het randomforestmodel. Lus --> GridSearch + Cross-validation
		 * + ParamGridBuilder
		 * 
		 */
		System.out.println();
		System.out.printf("\n_-* Random Forest Regression *-_\n");

		RandomForestRegressor rfr = new RandomForestRegressor().setLabelCol("AQI Value").setFeaturesCol("scaledFeatures");

		ParamMap[] paramGridRFR = new ParamGridBuilder()
				.addGrid(rfr.maxDepth(), new int[] {5, 10, 15, 20, 25, 30})
				.addGrid(rfr.numTrees(), new int[] {20, 40, 60, 80, 100, 120})
				.build();

		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, rfr });

		CrossValidator cv = new CrossValidator().setEstimator(pipelineRFR).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridRFR);

		CrossValidatorModel cvmRFR = cv.fit(sets[0]);

		System.out.printf("Beste model: %s\n", cvmRFR.bestModel().params().toString());
		
		Dataset<Row> cvPredictions = cvmRFR.transform(sets[1]);
		
		apreg.printRegressionEvaluation(cvPredictions);

		/*
		 * DecisionTree Regression
		 */
		System.out.printf("\n_-* Decision Tree Regressor *-_\n");

		DecisionTreeRegressor dtr = new DecisionTreeRegressor().setFeaturesCol("scaledFeatures").setLabelCol("AQI Value");

		Pipeline pipelineDT = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, dtr });

		ParamMap[] paramGridDT = new ParamGridBuilder().addGrid(dtr.maxDepth(), new int[] { 5, 10, 15, 20, 25, 30 })
				.build();

		CrossValidator cvDT = new CrossValidator().setEstimator(pipelineDT).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridDT);

		CrossValidatorModel cvmDT = cvDT.fit(sets[0]);

		System.out.printf("Beste model: %s\n", cvmDT.bestModel().params().toString());
		Dataset<Row> cvPredictionsDT = cvmDT.transform(sets[1]);
		apreg.printRegressionEvaluation(cvPredictionsDT);
		
		
		/*
		 * Generalized Lineaire Regressie
		 */
		System.out.printf("\n_-* Generalized Linear Regression *-_\n");
		
		GeneralizedLinearRegression glr = new GeneralizedLinearRegression().setFeaturesCol("scaledFeatures").setLabelCol("AQI Value").setFamily("gaussian");
		
		Pipeline pipelineGLR = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, glr });

		ParamMap[] paramGridGLR = new ParamGridBuilder()
				.addGrid(glr.maxIter(), new int[] {4, 8, 12, 16})
				.addGrid(glr.regParam(), new double[] {0.3, 0.6, 0.9})
				.build();
		
		CrossValidator cvGLR = new CrossValidator().setEstimator(pipelineGLR).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridGLR);

		CrossValidatorModel cvmGLR = cvGLR.fit(sets[0]);

		System.out.printf("Beste model: %s\n", cvmGLR.bestModel().params().toString());
		Dataset<Row> cvPredictionsGLR = cvmGLR.transform(sets[1]);
		apreg.printRegressionEvaluation(cvPredictionsGLR);	
	}
}
