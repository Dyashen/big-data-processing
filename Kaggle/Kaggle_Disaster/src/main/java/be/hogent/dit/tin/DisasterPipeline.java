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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

	private Dataset<Row> getLogRegPredictions(Dataset<Row>[] datasets, int iter) {
		LogisticRegression lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("target")
				.setMaxIter(iter);

		LogisticRegressionModel lrModel = lr.fit(datasets[0]);

		return lrModel.transform(datasets[1]);
	}

	private double getAccuracy(Dataset<Row> predictions) {
		return new MulticlassClassificationEvaluator().setLabelCol("target").setPredictionCol("prediction")
				.setMetricName("accuracy").evaluate(predictions);
	}

	private double getAreaROCCurve(Dataset<Row> predictions) {
		return new BinaryClassificationEvaluator().setLabelCol("target").evaluate(predictions);
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

		/* Model trainen met de 80%. */
		PipelineModel model = pipeline.fit(ml_df);

		Dataset<Row> final_data = model.transform(ml_df);

		/*
		 * 
		 * Dataframe opsplitsen in twee delen.
		 * 
		 */
		double[] verhouding = { 0.8, 0.2 };
		Dataset<Row>[] sets = nlp.splitSets(final_data, verhouding);

		/*
		 * 
		 * LOGISTIC REGRESSION
		 * 
		 */
		// int[] iterations_ints = {1, 10, 25, 50, 100};
		int[] iterations_ints = { 1, 3, 6, 9 };
		double maxAreaIter = 0;
		double maxAccuracyIter = 0;
		int idealAreaIter = 1;
		int idealAccIter = 1;

		for (int iter : iterations_ints) {
			Dataset<Row> logregPredictions = nlp.getLogRegPredictions(sets, iter);

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

		/*
		 * 
		 * DECISION TREE CLASSIFIER
		 * 
		 */
		double maxArea = 0;
		double maxAccuracy = 0;
		int idealLevelArea = 1;
		int idealLevelAcc = 1;

		for (int i = 1; i < 31; i++) {

			DecisionTreeClassifier dtc = new DecisionTreeClassifier().setFeaturesCol("features").setLabelCol("target")
					.setMaxDepth(i);

			DecisionTreeClassificationModel dtcm = dtc.fit(sets[0]);
			Dataset<Row> dtcPredictions = dtcm.transform(sets[1]);

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

		/*
		 * 
		 * RANDOM FOREST CLASSIFIER
		 * 
		 */
		
		System.out.println("-*-*- RANDOM FOREST CLASSIFIER -*-*-");

		int[] numTrees = {5, 15, 30, 45, 60, 75};
		int[] maxDepth = {15, 30, 45};

		for (int numTr : numTrees) {
			for (int maxD : maxDepth) {
				RandomForestClassifier rfc = new RandomForestClassifier().setNumTrees(numTr).setMaxDepth(maxD)
						.setLabelCol("target").setFeaturesCol("features").setSeed(42);
				
				RandomForestClassificationModel rfcm = rfc.fit(sets[0]);
				Dataset<Row> rfrPredictions = rfcm.transform(sets[1]);
				
				double currentArea = nlp.getAreaROCCurve(rfrPredictions);
				double currentAccuracy = nlp.getAccuracy(rfrPredictions);
				
				System.out.printf("%d %d : %.5f %.5f \n", numTr, maxD, currentArea, currentAccuracy);
			}
		}
	}
}
