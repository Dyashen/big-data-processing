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
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class DisasterPipeline {

	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();

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

	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	private Dataset<Row> getLogRegPredictions(Dataset<Row> trainingset, Dataset<Row> testset, int iter) {
		LogisticRegression lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("target")
				.setMaxIter(iter);

		LogisticRegressionModel lrModel = lr.fit(trainingset);

		return lrModel.transform(testset);
	}
	
	private Dataset<Row> getDecisionTreePredictions(Dataset<Row> trainingset, Dataset<Row> testset, int depth) {
		DecisionTreeClassifier dtc = new DecisionTreeClassifier().setFeaturesCol("features").setLabelCol("target")
				.setMaxDepth(depth);

		DecisionTreeClassificationModel dtcm = dtc.fit(trainingset);
		return dtcm.transform(testset);
	}
	
	private Dataset<Row> getRFCPredictions(Dataset<Row> trainingset, Dataset<Row> testset, int numTr, int maxDepth) {
		RandomForestClassifier rfc = new RandomForestClassifier().setNumTrees(numTr).setMaxDepth(maxDepth)
				.setLabelCol("target").setFeaturesCol("features").setSeed(42);
		
		RandomForestClassificationModel rfcm = rfc.fit(trainingset);
		return rfcm.transform(testset);
	}

	
	private double getAccuracy(Dataset<Row> predictions) {
		return new MulticlassClassificationEvaluator().setLabelCol("target").setPredictionCol("prediction")
				.setMetricName("accuracy").evaluate(predictions);
	}

	private double getAreaROCCurve(Dataset<Row> predictions) {
		return new BinaryClassificationEvaluator().setLabelCol("target").evaluate(predictions);
	}
	
	private void printConfusionMatrixEssence(Dataset<Row> predictions_and_labels) {
		Dataset<Row> preds_and_labels = predictions_and_labels
				.select("prediction", "target")
				.orderBy("prediction")
				.withColumn("target_d", col("target").cast("double"))
				.drop(col("target"));
		
		MulticlassMetrics metrics = new MulticlassMetrics(preds_and_labels);
		System.out.println(metrics.confusionMatrix());
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
	}

	public static void main(String[] args) {

		DisasterPipeline nlp = new DisasterPipeline();

		nlp.spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> training = nlp.getTrainingData();
		Dataset<Row> test = nlp.getTestData();

		Dataset<Row> ml_df = training.select(col("id"), col("text"), col("target"));
		ml_df = ml_df.na().drop();
		ml_df = ml_df.withColumn("str_only", regexp_replace(col("text"), "\\d+", ""));

		Pipeline pipeline = new Pipeline().setStages(
				new PipelineStage[] { nlp.getRegexTokenizer(), nlp.getStopWordsRemover(), nlp.getCountVectorizer() });

		PipelineModel model = pipeline.fit(ml_df);
		Dataset<Row> final_data = model.transform(ml_df);
		 
		
		Dataset<Row> test_df = test.select(col("id"), col("text"));
		test_df = test_df.withColumn("str_only", regexp_replace(col("text"), "\\d+", ""));
		
		Pipeline pipeline_test = new Pipeline().setStages(
				new PipelineStage[] { nlp.getRegexTokenizer(), nlp.getStopWordsRemover(), nlp.getCountVectorizer() });

		PipelineModel model_test = pipeline.fit(ml_df);
		Dataset<Row> final_test_data = model.transform(ml_df);

		/*
		 * LOGISTIC REGRESSION 
		 */
		
		// int[] iterations_ints = {1, 10, 25, 50, 100};
		int[] iterations_ints = { 1, 3, 6, 9 };
		double maxAreaIter = 0;
		double maxAccuracyIter = 0;
		int idealAreaIter = 1;
		int idealAccIter = 1;

		for (int iter : iterations_ints) {
			Dataset<Row> logregPredictions = nlp.getLogRegPredictions(final_data, final_test_data, idealAccIter);

			double currentArea = nlp.getAreaROCCurve(logregPredictions);
			double currentAccuracy = nlp.getAccuracy(logregPredictions);

			if (currentArea > maxAreaIter) {
				maxAreaIter = currentArea;
				idealAreaIter = iter;
			}

			if (currentAccuracy > maxAccuracyIter) {
				maxAccuracyIter = currentAccuracy;
				idealAccIter = iter;
			}
		}

		System.out.println("-*-*- LOGISTIC REGRESSION -*-*-");
		System.out.printf("Ideaal aantal iteraties %d met accuracy: %.5f \n", idealAccIter, maxAccuracyIter);
		System.out.printf("Ideaal aantal iteraties %d voor ROC Area: %.5f \n", idealAreaIter, maxAreaIter);
		
		Dataset<Row> logregIdealPredict = nlp.getLogRegPredictions(final_data, final_test_data, idealAccIter);
		nlp.printConfusionMatrixEssence(logregIdealPredict);
		System.out.println();


		/*
		 * 
		 * DECISION TREE CLASSIFIER
		 * 
		 */
		
		double maxArea = 0;
		double maxAccuracy = 0;
		int idealLevelArea = 1;
		int idealLevelAcc = 1;

		for (int i = 30; i >= 20; i--) {

			Dataset<Row> dtcPredictions = nlp.getDecisionTreePredictions(final_data, final_test_data, i);

			double currentArea = nlp.getAreaROCCurve(dtcPredictions);
			double currentAccuracy = nlp.getAccuracy(dtcPredictions);

			if (currentArea > maxArea) {
				maxArea = currentArea;
				idealLevelArea = i;
			}

			if (currentAccuracy > maxAccuracy) {
				maxAccuracy = currentAccuracy;
				idealLevelAcc = i;
			}
		}

		System.out.println("-*-*- DECISION TREE CLASSIFIER -*-*-");
		System.out.printf("Ideaal aantal levels voor accuracy: %d met %.5f \n", idealLevelAcc, maxAccuracy);
		System.out.printf("Ideaal aantal levels voor ROC Area: %d met %.5f \n", idealLevelArea, maxArea);
		
		Dataset<Row> detreeIdealPredict = nlp.getDecisionTreePredictions(final_data, final_test_data, idealLevelAcc);
		nlp.printConfusionMatrixEssence(detreeIdealPredict);
		System.out.println();

		
		 /* 
		 * RANDOM FOREST CLASSIFIER
		 */ 

		int[] numTrees = {5, 15, 30, 45, 60, 75};
		int[] maxDepth = {15, 20, 25, 30};

		int idealNrTrees = 5;
		int idealDepth = 15;
		
		for (int numTr : numTrees) {
			for (int maxD : maxDepth) {
				
				Dataset<Row> rfcPredictions = nlp.getRFCPredictions(final_data, final_test_data, numTr, maxD);
				
				double currentAccuracy = nlp.getAccuracy(rfcPredictions);
				
				if (currentAccuracy >= maxAccuracy) {
					maxAccuracy = currentAccuracy;
					idealNrTrees = numTr;
					idealDepth = maxD;
				}
			}
		}
		
		System.out.println("-*-*- RANDOM FOREST CLASSIFIER -*-*-");
		Dataset<Row> rfcPredictions = nlp.getRFCPredictions(final_data, final_test_data, idealNrTrees, idealDepth);
		nlp.printConfusionMatrixEssence(rfcPredictions);
		System.out.println();
		
	}
	
}
