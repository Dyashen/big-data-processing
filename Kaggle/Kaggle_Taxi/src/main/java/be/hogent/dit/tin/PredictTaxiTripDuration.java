package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PredictTaxiTripDuration {

	/*
	 * vaak voorkomende parameters bijhouden als final-object
	 */
	static final double[] verhouding = { 0.7, 0.3 };
	static final String label = "trip_duration";
	static final String prediction = "prediction";

	/*
	 * SparkSession
	 */
	static SparkSession spark = SparkSession.builder().appName("PredictTaxiTripDuration").master("local[*]")
			.config("spark.master", "local[*]").getOrCreate();

	
	/*
	 * Training & Testset ophalen
	 */
	private static Dataset<Row> getTraining() {
		return spark.read().option("header", true).csv("src/main/resources/train.csv");
	}

	private static Dataset<Row> getTest() {
		return spark.read().option("header", true).csv("src/main/resources/test.csv");
	}

	/*
	 * Dataset splitsen
	 */
	private static Dataset<Row>[] splitSets(Dataset<Row> dataset) {
		return dataset.randomSplit(verhouding);
	}

	
	/*
	 * Kolommen omzetten naar het juiste datatype.
	 * -- Timestamp moet een numeriek veld worden.
	 */
	private static Dataset<Row> clean(Dataset<Row> dataset) {
		dataset = dataset.na().drop();

		return dataset.withColumn("ts_pickup", unix_timestamp(col("pickup_datetime")))
				.withColumn("vendor_id", dataset.col("vendor_id").cast("double"))
				.withColumn("passenger_count", dataset.col("passenger_count").cast("double"))
				.withColumn("pickup_longitude", dataset.col("pickup_longitude").cast("double"))
				.withColumn("pickup_latitude", dataset.col("pickup_latitude").cast("double"))
				.withColumn("dropoff_longitude", dataset.col("dropoff_longitude").cast("double"))
				.withColumn("dropoff_latitude", dataset.col("dropoff_latitude").cast("double"))
				.drop("id", "pickup_datetime", "dropoff_datetime");
	}

	
	/*
	 * Correlatie matrix uitprinten.
	 */
	private static void printCorrelation(Dataset<Row> dataset) {
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
	
	
	/*
	 * Regressie evalueren op basis van vier metrieken.
	 */
	private static void printRegressionEvaluation(Dataset<Row> predictionsWithLabel) {
		String[] metricTypes = { "mse", "rmse", "r2", "mae" };
		System.out.printf("\n\nMetrics:\n");
		for (String metricType : metricTypes) {
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol(label)
					.setPredictionCol("prediction").setMetricName(metricType);

			double calc = evaluator.evaluate(predictionsWithLabel);
			
			System.out.printf("%s: \t%.5f \n", metricType, calc);
		}
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		/*
		 * Train & Test ophalen + cleanen.
		 */
		Dataset<Row> train = getTraining();
		train = clean(train);
		train = train.withColumn(label, train.col(label).cast("Integer"));

		Dataset<Row> test = getTest();
		test = clean(test);

		/*
		 * Categorische waarde omzetten naar numeriek veld.
		 */
		StringIndexer indexer = new StringIndexer().setHandleInvalid("keep").setInputCol("store_and_fwd_flag")
				.setOutputCol("flag_ind");

		/*
		 * StringIndexerModel indexerModel = indexer.fit(train); Dataset<Row>
		 * train_indexed = indexerModel.transform(train);
		 * 
		 * StringIndexerModel indexerModel = indexer.fit(test); Dataset<Row>
		 * test_indexed = indexerModel.transform(test);
		 */

		/*
		 * Alle features in één vector plaatsen.
		 */
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "vendor_id", "passenger_count", "pickup_longitude", "pickup_latitude",
						"dropoff_longitude", "dropoff_latitude", "ts_pickup", "flag_ind" })
				.setOutputCol("features");

		/*
		 * Eerste manier van scalen.
		 */
		MinMaxScaler minmax = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures");

		/*
		 * Tweede manier van scalen.
		 */
		StandardScaler stdScaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures");

		/*
		 * Evalueren bij crossvalidatie.
		 */
		RegressionEvaluator regEval = new RegressionEvaluator().setLabelCol(label).setMetricName("rmse");

		/*
		 * Lineaire regressie-object.
		 */
		LinearRegression linreg = new LinearRegression()
				.setLabelCol(label)
				.setFeaturesCol("scaledFeatures");

		Pipeline pipelineLinReg = new Pipeline()
				.setStages(new PipelineStage[] { indexer, assembler, stdScaler, linreg });

		Dataset<Row>[] datasets = splitSets(train);
		PipelineModel model = pipelineLinReg.fit(datasets[0]);
		Dataset<Row> trainedLinReg = model.transform(datasets[1]);
		
		/*
		 * Pick-up longitude + drop-off longitude hebben een sterke correlatie.
		 */
		printCorrelation(trainedLinReg);
		printRegressionEvaluation(trainedLinReg);

		Dataset<Row> predictionsLinReg = model.transform(test);
		predictionsLinReg.select(prediction).as("voorspelling LinReg").show(5);


		/*
		 * Random Forest Regressie
		 */
		System.out.printf("\n_-* Random Forest Regression *-_\n");
		RandomForestRegressor rfr =  new RandomForestRegressor().setLabelCol(label).setFeaturesCol("scaledFeatures");

		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, stdScaler, linreg });

		/*
		 * Parameter Grid for the Random Forest Regressor.
		 * Two parameters: the max depth + number of trees.
		 * Parameters were based of the standard value and finetuned with checking the optimal parameters.
		 */
		ParamMap[] paramGridRFR = new ParamGridBuilder()
				.addGrid(rfr.maxDepth(), new int[] {10, 20, 25, 30})
				.addGrid(rfr.numTrees(), new int[] {20, 40, 60, 80})
				.build();

		CrossValidator cvRFR = new CrossValidator()
				.setEstimator(pipelineRFR)
				.setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridRFR);
		
		CrossValidatorModel cvmRFR = cvRFR.fit(datasets[0]);
		Dataset<Row> rfrTrain = cvmRFR.transform(datasets[1]);
		
		printRegressionEvaluation(rfrTrain);
		
		Dataset<Row> predictionsRFR = cvmRFR.transform(test);
		
		predictionsRFR.select(prediction).as("voorspelling RFR").show(5);
		
		
		/*
		 * Generalized Linear Regression
		 */
		System.out.printf("\n_-* Generalized Linear Regression *-_\n");

		GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
				  .setFamily("gaussian")
				  .setLink("identity")
				  .setLabelCol(label)
				  .setFeaturesCol("scaledFeatures")
				  .setMaxIter(10)
				  .setRegParam(0.3);
		
		Pipeline pipelineGLR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, stdScaler, glr});
		
		ParamMap[] paramGridGLR = new ParamGridBuilder()
				.addGrid(glr.maxIter(), new int[] {15, 20, 25})
				.addGrid(glr.regParam(), new double[] {0.3, 0.6, 0.9})
				.build();
		
		TrainValidationSplit trainValidationSplitGLR = new TrainValidationSplit()
				.setEstimator(pipelineGLR)
				.setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridGLR)
				.setTrainRatio(0.8);

		TrainValidationSplitModel trainValidationSplitModelGLR = trainValidationSplitGLR.fit(datasets[0]);
		Dataset<Row> glrTrain = trainValidationSplitModelGLR.transform(datasets[1]);
		printRegressionEvaluation(glrTrain);
		
		Dataset<Row> predictionsGLR = trainValidationSplitModelGLR.transform(test);
		predictionsGLR.select(prediction).as("voorspelling GLR").show(5);
		
		
		/*
		 * Gradient-boost regressor
		 */
		System.out.printf("\n_-* Gradient Boost Regression *-_\n");

		GBTRegressor gbt = new GBTRegressor()
				  .setLabelCol(label)
				  .setFeaturesCol("scaledFeatures")
				  .setMaxIter(10);
		
		Pipeline pipelineGBT = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, stdScaler, gbt});
		
		ParamMap[] paramGridGBT = new ParamGridBuilder()
				.addGrid(gbt.maxIter(), new int[] {15, 20, 25})
				.build();
		
		CrossValidator cvGBT = new CrossValidator()
				.setEstimator(pipelineGBT)
				.setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridGBT);

		CrossValidatorModel cvmGBT = cvGBT.fit(datasets[0]);
		Dataset<Row> gbtTrain = cvmGBT.transform(datasets[1]);
		printRegressionEvaluation(gbtTrain);
		
		Dataset<Row> predictionsGBT = cvmGBT.transform(test);
		predictionsGLR.select(prediction).as("voorspelling GBT").show(5);
		
		/*
		 * Einde van de Spark-applicatie.
		 */
		spark.stop();
	}
}
