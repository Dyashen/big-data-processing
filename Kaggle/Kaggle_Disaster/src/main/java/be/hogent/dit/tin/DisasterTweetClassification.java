package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DisasterTweetClassification {
	
	final static String label = "target";
	final static double[] verhouding = {0.8,0.2};

	static SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();

	/*
	 * Een dataset opsplitsen naargelang een verhouding. De verhouding moeten twee
	 * doubles zijn: %training & %test.
	 */
	private static Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}
	
	private static Dataset<Row> createSubSample(Dataset<Row> dataset){
		dataset = dataset.sample(1.0);
		Dataset<Row> nietDisaster = dataset.where(col(label).equalTo(1));
		long lengte = nietDisaster.count();
		Dataset<Row> disaster = dataset.where(col(label).equalTo(0)).limit((int)lengte);
		dataset = nietDisaster.union(disaster);
		return dataset.sample(1.0);
	}

	private static Dataset<Row> getTrainingData() {
		return spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/train.csv");
	}

	private static Dataset<Row> getTestData() {
		return spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/test.csv");
	}

	private static double getAccuracy(Dataset<Row> predictions) {
		return new MulticlassClassificationEvaluator()
				.setLabelCol("target")
				.setPredictionCol("prediction")
				.setMetricName("accuracy")
				.evaluate(predictions);
	}

	private static double getAreaROCCurve(Dataset<Row> predictions) {
		return new BinaryClassificationEvaluator().setLabelCol("target").evaluate(predictions);
	}

	private static void printConfusionMatrixEssence(Dataset<Row> predictionsAndLabels) {
		Dataset<Row> predictionsAndLabel = predictionsAndLabels.select("prediction", label).orderBy("prediction")
				.withColumn("target_d", col(label).cast("double")).drop(col(label));

		MulticlassMetrics metrics = new MulticlassMetrics(predictionsAndLabel);
		System.out.println(metrics.confusionMatrix());
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> dataset = getTrainingData();
		Dataset<Row> testset = getTestData();
		
		// welke transformers moeten er nog bij?

		RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("str_only").setOutputCol("words").setPattern("\\W");
		StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
		CountVectorizer countVectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features");

		/*
		 * DATA-CLEANING Null-waarden weggooien + enkel letters behouden
		 */
		dataset = dataset.select(col("id"), col("text"), col("target"));
		dataset = dataset.na().drop();
		dataset = dataset.withColumn("str_only", regexp_replace(col("text"), "\\d+", ""));

		testset = testset.select(col("id"), col("text"));
		testset = testset.na().drop();
		testset = testset.withColumn("str_only", regexp_replace(col("text"), "\\d+", ""));

		/*
		 * Lichte geskewede dataset --> 50:50 ratio
		 */
		dataset = createSubSample(dataset);
		dataset.groupBy("target").count().show();
		
		/*
		 * PIPELINE
		 */
		Pipeline pipeline_training = new Pipeline()
				.setStages(new PipelineStage[] { regexTokenizer, stopWordsRemover, countVectorizer });

		PipelineModel modelTraining = pipeline_training.fit(dataset);
		Dataset<Row> convertedTraining = modelTraining.transform(dataset);

		PipelineModel modelTesting = pipeline_training.fit(testset);
		Dataset<Row> convertedTesting = modelTesting.transform(testset);

		Dataset<Row>[] datasets = splitSets(convertedTraining, verhouding);
		
		convertedTraining.show();
		convertedTesting.show();
		
		/*
		 * LOG-REG
		 */
		System.out.printf("\n\nLog-Reg\n");
		
		LogisticRegression lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("target");
		
		ParamMap[] paramGridLogReg = new ParamGridBuilder()
				.addGrid(lr.maxIter(), new int[] {10, 50, 100, 150, 300})
				.addGrid(lr.threshold(), new double[] {0.9})
				.build();
		
		CrossValidator cvLogReg = new CrossValidator()
				.setEstimator(lr)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label)) 
				.setEstimatorParamMaps(paramGridLogReg)
				//.setNumFolds(5)
				;
		
		CrossValidatorModel cvmLogReg = cvLogReg.fit(datasets[0]);
		System.out.println(cvmLogReg.paramMap().toString());
		System.out.printf("Beste model: %s\n", "");
		Dataset<Row> cvPredictionsLogReg = cvmLogReg.transform(datasets[1]);
		printConfusionMatrixEssence(cvPredictionsLogReg);	

		/*
		 * Random Forest Classifier
		 */
		System.out.printf("\n\nRandom Forest Classifier\n");
		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol("target")
				.setFeaturesCol("features").setSeed(42);
			
		ParamMap[] paramGridRFC = new ParamGridBuilder()
				.addGrid(rfc.maxDepth(), new int[] {15, 20, 25, 30})
				.addGrid(rfc.numTrees(), new int[] {60, 80, 100})
				.build();
		
		CrossValidator cvRFC = new CrossValidator()
				.setEstimator(rfc)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))  
				.setEstimatorParamMaps(paramGridRFC)
				.setNumFolds(5);

		CrossValidatorModel cvmRFC = cvRFC.fit(datasets[0]);
		System.out.printf("Beste model: %s\n", cvmRFC.bestModel().params().toString());
		Dataset<Row> cvPredictionsRFC = cvmRFC.transform(datasets[1]);
		printConfusionMatrixEssence(cvPredictionsRFC);
	}
}