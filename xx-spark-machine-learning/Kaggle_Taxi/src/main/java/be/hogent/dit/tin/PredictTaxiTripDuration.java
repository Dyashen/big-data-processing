package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.dayofweek;

import java.lang.Math;
import java.util.Arrays;
import java.util.List;

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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
			.config("spark.master", "local[1]").getOrCreate();

	/*
	 * Training & Testset ophalen
	 */
	private static Dataset<Row> getTraining() {

		List<StructField> fields = Arrays.asList(DataTypes.createStructField("id", DataTypes.StringType, false),
				DataTypes.createStructField("vendor_id", DataTypes.DoubleType, false),
				DataTypes.createStructField("pickup_datetime", DataTypes.TimestampType, false), // TimestampType
				DataTypes.createStructField("dropoff_datetime", DataTypes.TimestampType, false),
				DataTypes.createStructField("passenger_count", DataTypes.DoubleType, false),
				DataTypes.createStructField("pickup_longitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("pickup_latitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("dropoff_longitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("dropoff_latitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("store_and_fwd_flag", DataTypes.StringType, false),
				DataTypes.createStructField("trip_duration", DataTypes.DoubleType, false));

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> dataset = spark.read().option("header", true).schema(schema).csv("src/main/resources/train.csv");

		return dataset.withColumn("hour", hour(col("pickup_datetime")))
				.withColumn("day", dayofweek(col("pickup_datetime"))).drop("id", "pickup_datetime", "dropoff_datetime");
	}

	private static Dataset<Row> getTest() {

		List<StructField> fields = Arrays.asList(DataTypes.createStructField("id", DataTypes.StringType, false),
				DataTypes.createStructField("vendor_id", DataTypes.DoubleType, false),
				DataTypes.createStructField("pickup_datetime", DataTypes.TimestampType, false),
				DataTypes.createStructField("passenger_count", DataTypes.DoubleType, false),
				DataTypes.createStructField("pickup_longitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("pickup_latitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("dropoff_longitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("dropoff_latitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("store_and_fwd_flag", DataTypes.StringType, false));

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> dataset = spark.read().option("header", true).schema(schema).csv("src/main/resources/test.csv");

		return dataset.withColumn("hour", hour(col("pickup_datetime")))
				.withColumn("day", dayofweek(col("pickup_datetime"))).drop("id", "pickup_datetime");
	}

	/*
	 * Dataset splitsen
	 */
	private static Dataset<Row>[] splitSets(Dataset<Row> dataset) {
		return dataset.randomSplit(verhouding);
	}

	/*
	 * Kolommen omzetten naar het juiste datatype. -- Timestamp moet een numeriek
	 * veld worden.
	 */
	private static Dataset<Row> clean(Dataset<Row> dataset) {
		dataset = dataset.na().drop();

		double latMin = 40.6;
		double latMax = 40.9;
		double longMin = -74.25;
		double longMax = -73.7;

		/*
		 * REMOVE OUTLIERS FOR LONGITUDE/LATITUDE
		 */
		dataset = dataset.where(col("pickup_longitude").$greater$eq(longMin))
				.where(col("dropoff_longitude").$greater$eq(longMin)).where(col("pickup_latitude").$greater$eq(latMin))
				.where(col("dropoff_latitude").$greater$eq(latMin)).where(col("pickup_longitude").$less$eq(longMax))
				.where(col("dropoff_longitude").$less$eq(longMax)).where(col("pickup_latitude").$less$eq(latMax))
				.where(col("dropoff_latitude").$less$eq(latMax));

		/*
		 * Only the rows with a correct passenger count.
		 */
		dataset = dataset.where(col("passenger_count").$greater(0));

		return dataset;
	}

	// UDF4
	private static Dataset<Row> addDistance(Dataset<Row> dataset) {
		return dataset
				.withColumn("distance",
						call_udf("haversine", col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"),
								col("dropoff_longitude")))
				.drop("pickup_longitude", "dropoff_longitude", "pickup_latitude", "dropoff_latitude");
	}

	// UDF2
	private static Dataset<Row> addSpeed(Dataset<Row> dataset) {
		return dataset.withColumn("speed", call_udf("speed", col("distance"), col("trip_duration")));
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
	 * Vier scores worden uitgeprint.
	 */
	private static void printRegressionEvaluation(Dataset<Row> predictionsWithLabel) {
		String[] metricTypes = { "mse", "rmse", "r2", "mae" };
		System.out.printf("\n\nMetrics:\n");
		for (String metricType : metricTypes) {
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction")
					.setMetricName(metricType);

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

		Dataset<Row> test = getTest();
		test = clean(test);

		/*
		 * SHOW THE AMOUNT OF TRIPS PER HOUR
		 */
		System.out.println("SHOW THE AMOUNT OF TRIPS PER HOUR");
		train.groupBy("hour").count().orderBy("hour").show();

		System.out.println("SHOW THE AMOUNT OF TRIPS PER DAY");
		train.groupBy("day").count().orderBy("day").show();

		// Haversine
		// Wordt betrokken in het model.
		UDF4<Double, Double, Double, Double, Double> haversine = new UDF4<Double, Double, Double, Double, Double>() {
			public Double call(Double pickupLatitude, Double pickupLongitude, Double dropoffLatitude,
					Double dropoffLongitude) throws Exception {

				double radius = 6371 * 1.1; // Radius van de Aarde

				double lat1 = pickupLatitude;
				double lon1 = pickupLongitude;

				double lat2 = dropoffLatitude;
				double lon2 = dropoffLongitude;

				double deltaLat = Math.toRadians(lat2 - lat1);
				double deltaLon = Math.toRadians(lon2 - lon1);

				// Haversine formula om de afstand tussen longitude en latitude te berekenen.
				double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) + Math.cos(Math.toRadians(lat1))
						* Math.cos(Math.toRadians(lat2)) * Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);

				double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

				double d = radius * c;

				return d;
			}
		};

		// Enkel voor statistiekgebruik.
		UDF2<Double, Double, Double> speed = new UDF2<Double, Double, Double>() {
			public Double call(Double distance, Double time) throws Exception {
				return distance / (time / 3600);
			}
		};

		spark.udf().register("haversine", haversine, DataTypes.DoubleType);
		spark.udf().register("speed", speed, DataTypes.DoubleType);

		train = addDistance(train);
		train = addSpeed(train);

		test = addDistance(test);

		/*
		 * Toon de gemiddelde afstand per huur / dag
		 */
		System.out.println("SHOW THE MEAN SPEED OF TRIPS PER HOUR");
		train.groupBy("hour").mean("speed").orderBy("hour").show();

		System.out.println("SHOW THE MEAN SPEED OF TRIPS PER DAY");
		train.groupBy("day").mean("speed").orderBy("day").show();

		/*
		 * Toon de gemiddelde afstand per huur / dag
		 */
		System.out.println("SHOW THE MEAN DISTANCE OF TRIPS PER HOUR");
		train.groupBy("hour").mean("distance").orderBy("hour").show();

		System.out.println("SHOW THE MEAN DISTANCE OF TRIPS PER DAY");
		train.groupBy("day").mean("distance").orderBy("day").show();

		/*
		 * Categorische waarde omzetten naar numeriek veld.
		 */
		StringIndexer indexer = new StringIndexer().setHandleInvalid("keep").setInputCol("store_and_fwd_flag")
				.setOutputCol("flag_ind");

		/*
		 * Alle features in één vector plaatsen.
		 */
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "vendor_id", "passenger_count", "hour", "day", "flag_ind", "distance" })
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
		LinearRegression linreg = new LinearRegression().setLabelCol(label).setFeaturesCol("scaledFeatures");

		Pipeline pipelineLinReg = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, minmax, linreg });

		Dataset<Row>[] datasets = train.randomSplit(verhouding);
		PipelineModel model = pipelineLinReg.fit(datasets[0]);
		Dataset<Row> trainedLinReg = model.transform(datasets[1]);

		trainedLinReg.show();

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
		RandomForestRegressor rfr = new RandomForestRegressor().setLabelCol(label).setFeaturesCol("scaledFeatures");

		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, stdScaler, linreg });

		/*
		 * Parameter Grid for the Random Forest Regressor. Two parameters: the max depth
		 * + number of trees. Parameters were based of the standard value and finetuned
		 * with checking the optimal parameters.
		 */
		ParamMap[] paramGridRFR = new ParamGridBuilder().addGrid(rfr.maxDepth(), new int[] { 10, 20, 25, 30 })
				.addGrid(rfr.numTrees(), new int[] { 20, 40, 60, 80 }).build();

		CrossValidator cvRFR = new CrossValidator().setEstimator(pipelineRFR).setEvaluator(regEval)
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

		GeneralizedLinearRegression glr = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity")
				.setLabelCol(label).setFeaturesCol("scaledFeatures").setMaxIter(10).setRegParam(0.3);

		Pipeline pipelineGLR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, stdScaler, glr });

		ParamMap[] paramGridGLR = new ParamGridBuilder().addGrid(glr.maxIter(), new int[] { 15, 20, 25 })
				.addGrid(glr.regParam(), new double[] { 0.3, 0.6, 0.9 }).build();

		TrainValidationSplit trainValidationSplitGLR = new TrainValidationSplit().setEstimator(pipelineGLR)
				.setEvaluator(regEval).setEstimatorParamMaps(paramGridGLR).setTrainRatio(0.8);

		TrainValidationSplitModel trainValidationSplitModelGLR = trainValidationSplitGLR.fit(datasets[0]);
		Dataset<Row> glrTrain = trainValidationSplitModelGLR.transform(datasets[1]);
		printRegressionEvaluation(glrTrain);

		Dataset<Row> predictionsGLR = trainValidationSplitModelGLR.transform(test);
		predictionsGLR.select(prediction).as("voorspelling GLR").show(5);

		/*
		 * Gradient-boost regressor
		 */
		System.out.printf("\n_-* Gradient Boost Regression *-_\n");

		GBTRegressor gbt = new GBTRegressor().setLabelCol(label).setFeaturesCol("scaledFeatures").setMaxIter(10);

		Pipeline pipelineGBT = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, stdScaler, gbt });

		ParamMap[] paramGridGBT = new ParamGridBuilder().addGrid(gbt.maxIter(), new int[] { 15, 20, 25 }).build();

		CrossValidator cvGBT = new CrossValidator().setEstimator(pipelineGBT).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridGBT);

		CrossValidatorModel cvmGBT = cvGBT.fit(datasets[0]);
		Dataset<Row> gbtTrain = cvmGBT.transform(datasets[1]);
		printRegressionEvaluation(gbtTrain);

		Dataset<Row> predictionsGBT = cvmGBT.transform(test);
		predictionsGBT.select(prediction).as("voorspelling GBT").show(5);

		spark.stop();
	}
}
