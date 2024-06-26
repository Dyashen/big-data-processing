package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofweek;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.rand;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.when;

//import static org.apache.spark.sql.DataFrameStatFunctions.approxQuantile;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TaxiTripRegression {
	/*
	 * vaak voorkomende parameters bijhouden als final-object
	 */
	static final double[] verhouding = { 0.8, 0.2 };
	static final String label = "trip_duration";
	static final String prediction = "prediction";
	static final String metric = "rmse";

	/*
	 * SparkSession
	 */
	static SparkSession spark = SparkSession.builder().appName("TaxiTrip").master("local[1]")
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
				.withColumn("day", dayofweek(col("pickup_datetime"))).drop("pickup_datetime", "dropoff_datetime");
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
				.withColumn("day", dayofweek(col("pickup_datetime"))).drop("pickup_datetime");
	}

	private static Dataset<Row> clean(Dataset<Row> dataframe) {

		// alle rijen met nullwaarden droppen
		dataframe = dataframe.na().drop();

		double latMin = 40.6;
		double latMax = 40.9;
		double longMin = -74.25;
		double longMax = -73.7;

		// longitude en latitude bereik aanpassen
		dataframe = dataframe.where(col("pickup_longitude").$greater$eq(longMin))
				.where(col("dropoff_longitude").$greater$eq(longMin)).where(col("pickup_latitude").$greater$eq(latMin))
				.where(col("dropoff_latitude").$greater$eq(latMin)).where(col("pickup_longitude").$less$eq(longMax))
				.where(col("dropoff_longitude").$less$eq(longMax)).where(col("pickup_latitude").$less$eq(latMax))
				.where(col("dropoff_latitude").$less$eq(latMax));

		// outliers voor de afstand verwijderen --> werken met [ avg - (1*std) , avg +
		// (1*std)]
		double rightOuter = dataframe.select(avg(col("distance")).plus(stddev(col("distance")))).first().getDouble(0);

		double leftOuter = dataframe.select(avg(col("distance")).minus(stddev(col("distance")))).first().getDouble(0);

		dataframe = dataframe.where(col("distance").$less$eq(rightOuter)).where(col("distance").$greater$eq(leftOuter));

		// double q1 = dataset.approxQuantile("column_name", 0.25, 0);
		// double q3 = dataset.approxQuantile("column_name", 0.75, 0);
		// dataset =
		// dataset.where(col("distance").$less$eq(q3)).where(col("distance").$greater$eq(q1));

		// enkel taxi's met inzittenden
		dataframe = dataframe.where(col("passenger_count").$greater(0));

		// dataframe willekeurig ordenen
		dataframe = dataframe.orderBy(rand());

		return dataframe;
	}

	// UDF4
	private static Dataset<Row> addDistance(Dataset<Row> dataframe) {
		return dataframe
				.withColumn("distance",
						call_udf("haversine", col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"),
								col("dropoff_longitude")))
				.drop("pickup_longitude", "dropoff_longitude", "pickup_latitude", "dropoff_latitude");
	}

	// UDF2
	private static Dataset<Row> addSpeed(Dataset<Row> dataframe) {
		return dataframe.withColumn("speed", call_udf("speed", col("distance"), col(label)));
	}

	private static void printCorrelation(Dataset<Row> dataframe) {
		Row r1 = Correlation.corr(dataframe, "features").head();
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
	private static void printRegressionEvaluation(Dataset<Row> dataframe) {
		String[] metricTypes = { "mse", "rmse", "r2", "mae" };
		System.out.printf("\n\nMetrics:\n");
		for (String metricType : metricTypes) {
			RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
					.setMetricName(metricType);

			double calc = evaluator.evaluate(dataframe);

			System.out.printf("%s: \t%.5f \n", metricType, calc);
		}

		RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
				.setMetricName(metric);

		double calc = evaluator.evaluate(dataframe);
		double log = Math.log(calc);
		System.out.printf("%s: \t%.5f \n", "rmsle", log);

	}

	private static Dataset<Row> getRangeDataFrame(Dataset<Row> dataframe) {
		dataframe = dataframe.withColumn("margin", abs(col("prediction").minus(col("trip_duration"))));

		Dataset<Row> range = dataframe.withColumn("range",
				when(col("margin").leq(50), "0-50").when(col("margin").leq(200), "50-200")
						.when(col("margin").leq(500), "200-500").when(col("margin").leq(5000), "500-5000")
						.otherwise("5000+"));

		return range.groupBy("range").count();
	}

	private static void createSubmission(Dataset<Row> dataframe, String fnaam) {
		String output = "src/main/resources/" + fnaam + ".csv";
		dataframe = dataframe.select("id", "prediction");
		dataframe.write().mode(SaveMode.Overwrite).csv(output);
	}

	public static void main(String[] args) {

		// Enkel errors tonen
		spark.sparkContext().setLogLevel("ERROR");

		// Dataset ophalen
		Dataset<Row> train = getTraining();
		Dataset<Row> test = getTest();

		// Statistieken tonen. Hoeveel trips waren er per dag/uur?
		train.groupBy("hour").count().orderBy("hour").show();
		test.groupBy("hour").count().orderBy("hour").show();
		train.groupBy("hour").count().orderBy("hour").show();
		test.groupBy("hour").count().orderBy("hour").show();

		// Distance toevoegen
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

				// Formule om de grootcirkelafstand te berekenen.
				double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) + Math.cos(Math.toRadians(lat1))
						* Math.cos(Math.toRadians(lat2)) * Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);

				double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
				double d = radius * c;
				return d;
			}
		};

		// Speed toevoegen
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

		train.show();
		test.show();

		// Wat is de gemiddelde snelheid?
		train.groupBy("hour").mean("speed").orderBy("hour").show();
		train.groupBy("day").mean("speed").orderBy("day").show();

		// Hoeveel kilometer werd er op dat uur afgelegd?
		train.groupBy("hour").sum("distance").orderBy("hour").show();
		train.groupBy("day").sum("distance").orderBy("day").show();

		train = clean(train);
		test = clean(test);

		// Outliers verwijderen op basis van elapsed time
		double rightOuter = train.select(avg(col(label)).plus(stddev(col(label)))).first().getDouble(0);
		double leftOuter = train.select(avg(col(label)).minus(stddev(col(label)))).first().getDouble(0);
		train = train.where(col(label).$less$eq(rightOuter)).where(col(label).$greater$eq(leftOuter));

		Dataset<Row>[] datasets = train.randomSplit(verhouding, 42);
		Dataset<Row> trainSetSplit = datasets[0];
		Dataset<Row> trainTestSplit = datasets[1];
		System.out.printf("Lengte trainingset: %d\n", trainSetSplit.count());
		System.out.printf("Lengte training-testset: %d\n", trainTestSplit.count());

		/*
		 * Lineaire regressie
		 */
		StringIndexer indexer = new StringIndexer().setHandleInvalid("keep").setInputCol("store_and_fwd_flag")
				.setOutputCol("flag_ind");

		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "vendor_id", "passenger_count", "hour", "day", "flag_ind", "distance" })
				.setOutputCol("features");

		MinMaxScaler minMax = new MinMaxScaler().setInputCol(assembler.getOutputCol()).setOutputCol("scaledFeatures");
		LinearRegression linReg = new LinearRegression().setLabelCol(label).setFeaturesCol(minMax.getOutputCol());
		Pipeline pipelineLinReg = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, minMax, linReg });

		// Het model trainen
		PipelineModel model = pipelineLinReg.fit(trainSetSplit);
		Dataset<Row> trainedLinReg = model.transform(trainTestSplit);

		// Metrieken uitprinten
		printCorrelation(trainedLinReg);
		printRegressionEvaluation(trainedLinReg);

		// Hoe groot is de marge tussen onze voorspelde waarden en de effectieve
		// waarden?
		getRangeDataFrame(trainedLinReg).show();

		// Voorspellingen maken + eerste vijf tonen + naar csv
		Dataset<Row> predictionsLinReg = model.transform(test);
		predictionsLinReg.show();
		createSubmission(predictionsLinReg, "submissionLinearRegression");

		// Nodig voor de volgende modellen
		RegressionEvaluator regEval = new RegressionEvaluator().setLabelCol(label).setMetricName(metric);

		/*
		 * Generalized Linear Regression
		 */
		System.out.printf("\n_-* Generalized Linear Regression *-_\n");

		GeneralizedLinearRegression glr = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity")
				.setLabelCol(label).setFeaturesCol(minMax.getOutputCol()).setMaxIter(10).setRegParam(0.3);

		ParamMap[] paramGridGLR = new ParamGridBuilder().addGrid(glr.maxIter(), new int[] { 15, 20, 25 })
				.addGrid(glr.regParam(), new double[] { 0.6, 0.9 }).build();

		TrainValidationSplit trainValidationSplitGLR = new TrainValidationSplit().setEstimator(glr)
				.setEvaluator(regEval).setEstimatorParamMaps(paramGridGLR).setTrainRatio(verhouding[0]);

		Pipeline pipelineGLR = new Pipeline()
				.setStages(new PipelineStage[] { indexer, assembler, minMax, trainValidationSplitGLR });

		PipelineModel pipelineModelGLR = pipelineGLR.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nMax Iter: %s\nReg params: %s",
				trainValidationSplitGLR.getEstimatorParamMaps()[0].get(glr.maxIter()).toString(),
				trainValidationSplitGLR.getEstimatorParamMaps()[1].get(glr.regParam()).toString());

		Dataset<Row> trainedGLR = pipelineModelGLR.transform(trainTestSplit);

		printRegressionEvaluation(trainedGLR);
		getRangeDataFrame(trainedGLR).show();

		// Voorspellingen maken + eerste vijf tonen + naar csv
		Dataset<Row> predictionsGLR = pipelineModelGLR.transform(test);
		predictionsGLR.select(prediction).as("voorspelling GLR").show(5);
		createSubmission(predictionsGLR, "submissionGLR");

		/*
		 * Gradient-boost regressor
		 */
		System.out.printf("\n_-* Gradient Boost Regression *-_\n");

		GBTRegressor gbt = new GBTRegressor().setLabelCol(label).setFeaturesCol(minMax.getOutputCol()).setMaxIter(10);

		ParamMap[] paramGridGBT = new ParamGridBuilder().addGrid(gbt.maxIter(), new int[] { 15, 20, 25 }).build();

		TrainValidationSplit trainValidationSplitGBT = new TrainValidationSplit().setEstimator(gbt)
				.setEvaluator(regEval).setEstimatorParamMaps(paramGridGBT).setTrainRatio(verhouding[0]);

		Pipeline pipelineGBT = new Pipeline()
				.setStages(new PipelineStage[] { indexer, assembler, minMax, trainValidationSplitGBT });

		PipelineModel pipelineModelGBT = pipelineGBT.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nMax Iter: %s",
				trainValidationSplitGBT.getEstimatorParamMaps()[0].get(gbt.maxIter()).toString());

		Dataset<Row> trainedGBT = pipelineModelGBT.transform(trainTestSplit);

		printRegressionEvaluation(trainedGBT);
		getRangeDataFrame(trainedGBT).show();

		// Voorspellingen maken + eerste vijf tonen + naar csv
		Dataset<Row> predictionsGBT = pipelineModelGBT.transform(test);
		predictionsGBT.select(prediction).as("voorspelling GBT").show(5);
		createSubmission(predictionsGBT, "submissionGBT");

		/*
		 * Random Forest Regressie
		 */
		System.out.printf("\n_-* Random Forest Regression *-_\n");
		RandomForestRegressor rfr = new RandomForestRegressor().setLabelCol(label)
				.setFeaturesCol(minMax.getOutputCol());

		ParamMap[] paramGridRFR = new ParamGridBuilder().addGrid(rfr.maxDepth(), new int[] { 5, 10, 15 })
				.addGrid(rfr.numTrees(), new int[] { 50, 80 }).build();

		CrossValidator cvRFR = new CrossValidator().setEstimator(rfr).setEvaluator(regEval)
				.setEstimatorParamMaps(paramGridRFR);

		Pipeline pipelineRFR = new Pipeline().setStages(new PipelineStage[] { indexer, assembler, minMax, cvRFR });

		PipelineModel pipelineModelRFR = pipelineRFR.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nMax depth: %s\nNumber of trees: %s",
				cvRFR.getEstimatorParamMaps()[0].get(rfr.maxDepth()).toString(),
				cvRFR.getEstimatorParamMaps()[1].get(rfr.numTrees()).toString());

		Dataset<Row> trainedRFR = pipelineModelRFR.transform(trainTestSplit);

		printRegressionEvaluation(trainedRFR);
		getRangeDataFrame(trainedRFR).show();

		Dataset<Row> predictionsRFR = pipelineModelRFR.transform(test);
		predictionsRFR.select(prediction).as("voorspelling RFR").show(5);

		// csv maken met ID + prediction
		createSubmission(predictionsRFR, "submissionRFR");

		spark.stop();
	}

}
