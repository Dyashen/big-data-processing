package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassificationSummary;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class DisasterTweetClassification {

	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();

	/*
	 * Een dataset opsplitsen naargelang een verhouding. De verhouding moeten twee
	 * doubles zijn: %training & %test.
	 */
	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	private Dataset<Row> getTrainingData() {
		return this.spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/train.csv");
	}

	private Dataset<Row> getTestData() {
		return this.spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/test.csv");
	}

	private RegexTokenizer getRegexTokenizer() {
		return new RegexTokenizer().setInputCol("str_only").setOutputCol("words").setPattern("\\W");
	}

	private StopWordsRemover getStopWordsRemover() {
		return new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
	}

	private CountVectorizer getCountVectorizer() {
		return new CountVectorizer().setInputCol("filtered").setOutputCol("features");
	}
	
	private RandomForestClassifier getRFRModel() {
		return new RandomForestClassifier().setLabelCol("target").setFeaturesCol("features").setSeed(42);
	}

	private double getAccuracy(Dataset<Row> predictions) {
		return new MulticlassClassificationEvaluator().setLabelCol("target").setPredictionCol("prediction")
				.setMetricName("accuracy").evaluate(predictions);
	}

	private double getAreaROCCurve(Dataset<Row> predictions) {
		return new BinaryClassificationEvaluator().setLabelCol("target").evaluate(predictions);
	}

	private void printConfusionMatrixEssence(Dataset<Row> predictions_and_labels) {
		Dataset<Row> preds_and_labels = predictions_and_labels.select("prediction", "target").orderBy("prediction")
				.withColumn("target_d", col("target").cast("double")).drop(col("target"));

		MulticlassMetrics metrics = new MulticlassMetrics(preds_and_labels);
		System.out.println(metrics.confusionMatrix());
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
	}

	public static void main(String[] args) {

		DisasterTweetClassification nlp = new DisasterTweetClassification();
		nlp.spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> dataset = nlp.getTrainingData();
		Dataset<Row> testset = nlp.getTestData();

		RegexTokenizer regexTokenizer = nlp.getRegexTokenizer();
		StopWordsRemover stopWordsRemover = nlp.getStopWordsRemover();
		CountVectorizer countVectorizer = nlp.getCountVectorizer();

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
		 * PIPELINE
		 */
		Pipeline pipeline_training = new Pipeline()
				.setStages(new PipelineStage[] { regexTokenizer, stopWordsRemover, countVectorizer });

		/*
		 * ParamMap[] paramGrid = new ParamGridBuilder()
		 * .addGrid(countVectorizer.minDF(), new double[] {0.2, 0.6, 1.0})
		 * .addGrid(countVectorizer.maxDF(), new double[] {1.0, 2.0}).build();
		 */

		PipelineModel modelTraining = pipeline_training.fit(dataset);
		Dataset<Row> converted_training = modelTraining.transform(dataset);

		PipelineModel modelTesting = pipeline_training.fit(testset);
		Dataset<Row> converted_testing = modelTraining.transform(testset);

		Dataset<Row>[] datasets = nlp.splitSets(converted_training, new double[] { 0.7, 0.3 });

		/*
		 * LOG-REG
		 */
		System.out.printf("\n\nLog-Reg\n");
		
		LogisticRegression lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("target");
		
		ParamMap[] paramGridLogReg = new ParamGridBuilder()
				.addGrid(lr.maxIter(), new int[] {4, 8, 12, 16})
				.addGrid(lr.threshold(), new double[] {0.3, 0.6, 0.9})
				.build();
		
		CrossValidator cvLogReg = new CrossValidator()
				.setEstimator(lr)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("target")) // metricname? 
				.setEstimatorParamMaps(paramGridLogReg);
		
		CrossValidatorModel cvmLogReg = cvLogReg.fit(datasets[0]);

		System.out.printf("Beste model: %s\n", cvmLogReg.bestModel().params().toString());
		Dataset<Row> cvPredictionsLogReg = cvmLogReg.transform(datasets[1]);
		nlp.printConfusionMatrixEssence(cvPredictionsLogReg);	

		
		/*
		 * Random Forest Classifier
		 */
		System.out.printf("\n\nRandom Forest Classifier\n");
		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol("target")
				.setFeaturesCol("features").setSeed(42);
			
		ParamMap[] paramGridRFC = new ParamGridBuilder()
				.addGrid(rfc.maxDepth(), new int[] {5, 10, 15, 20, 25, 30})
				.addGrid(rfc.numTrees(), new int[] {20, 40, 60, 80, 100, 120})
				.build();
		
		CrossValidator cvRFC = new CrossValidator()
				.setEstimator(rfc)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("target")) // metricname? 
				.setEstimatorParamMaps(paramGridRFC);

		CrossValidatorModel cvmRFC = cvRFC.fit(datasets[0]);
		System.out.printf("Beste model: %s\n", cvmRFC.bestModel().params().toString());
		Dataset<Row> cvPredictionsRFC = cvmRFC.transform(datasets[1]);
		nlp.printConfusionMatrixEssence(cvPredictionsRFC);
	}
}
