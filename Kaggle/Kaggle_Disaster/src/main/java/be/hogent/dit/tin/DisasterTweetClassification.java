package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DisasterTweetClassification {

	final static String label = "target";
	final static double[] verhouding = { 0.8, 0.2 };

	static SparkSession spark = SparkSession.builder().appName("DisasterTweet").master("local[1]")
			.getOrCreate();

	/*
	 * Een dataset opsplitsen naargelang een verhouding. De verhouding moeten twee
	 * doubles zijn: %training & %test.
	 */
	private static Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	private static Dataset<Row> createSubSample(Dataset<Row> dataset) {
		dataset = dataset.sample(1.0);
		Dataset<Row> nietDisaster = dataset.where(col(label).equalTo(1));
		long lengte = nietDisaster.count();
		Dataset<Row> disaster = dataset.where(col(label).equalTo(0)).limit((int) lengte);
		dataset = nietDisaster.union(disaster);
		return dataset.sample(1.0);
	}

	private static Dataset<Row> getTrainingData() {

		List<StructField> fields = Arrays.asList(DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("keyword", DataTypes.StringType, false),
				DataTypes.createStructField("location", DataTypes.StringType, false), // TimestampType
				DataTypes.createStructField("text", DataTypes.StringType, false),
				DataTypes.createStructField("target", DataTypes.DoubleType, false));

		StructType schema = DataTypes.createStructType(fields);

		return spark.read().option("header", true).schema(schema).csv("src/main/resources/train.csv");
	}

	private static Dataset<Row> getTestData() {

		List<StructField> fields = Arrays.asList(DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("keyword", DataTypes.StringType, false),
				DataTypes.createStructField("location", DataTypes.StringType, false), // TimestampType
				DataTypes.createStructField("text", DataTypes.StringType, false));

		StructType schema = DataTypes.createStructType(fields);

		return spark.read().option("header", true).schema(schema).csv("src/main/resources/test.csv");
	}

	private static double getAreaROCCurve(Dataset<Row> predictions) {
		return new BinaryClassificationEvaluator().setLabelCol(label).evaluate(predictions);
	}

	private static void printConfusionMatrixEssence(Dataset<Row> predictionsAndLabels) {
		System.out.println();
		Dataset<Row> predictionsAndLabel = predictionsAndLabels.select("prediction", label).orderBy("prediction")
				.withColumn("target_d", col(label).cast("double")).drop(col(label));

		MulticlassMetrics metrics = new MulticlassMetrics(predictionsAndLabel);
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
		System.out.printf("Nauwkeurigheid: %.5f \n", metrics.accuracy());
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> dataset = getTrainingData();
		Dataset<Row> testset = getTestData();

		
		dataset.show();
		testset.show();

		// ...
		dataset = dataset.select(col("id"), col("text"), col(label));
		dataset = dataset.na().drop();
		dataset = dataset.withColumn("text", regexp_replace(col("text"), "\\d+", ""));

		testset = testset.select(col("id"), col("text"));
		testset = testset.na().drop();
		testset = testset.withColumn("text", regexp_replace(col("text"), "\\d+", ""));

		// ...
		dataset = createSubSample(dataset);
		dataset.groupBy(label).count().show();
		
		Dataset<Row>[] datasets = splitSets(dataset, verhouding);
		Dataset<Row> trainSetSplit = datasets[0];
		Dataset<Row> testSetSplit = datasets[1];
		
		System.out.printf("Lengte trainsetsplit: %s\n", trainSetSplit.count());
		System.out.printf("Lengte testsetsplit: %s\n", testSetSplit.count());

		// Pipeline
		RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("words").setPattern("\\W");

		StopWordsRemover remover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol())
				.setOutputCol("filtered");

		CountVectorizer vectorizer = new CountVectorizer().setInputCol(remover.getOutputCol()).setOutputCol("features")
				.setVocabSize(10000).setMinDF(5);

		HashingTF hashingTF = new HashingTF().setInputCol(remover.getOutputCol()).setOutputCol("hashedFeatures");
		
		LogisticRegression lr = new LogisticRegression()
				.setFeaturesCol(hashingTF.getOutputCol())
				.setLabelCol(label)
				.setMaxIter(10)
				.setRegParam(0.001);

		ParamMap[] paramGridLogReg = new ParamGridBuilder()
				.addGrid(lr.maxIter(), new int[] { 400 })
				.addGrid(lr.threshold(), new double[] { 0.7, 0.8, 0.9 })
				.build();

		CrossValidator cvLogReg = new CrossValidator()
				.setEstimator(lr)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridLogReg);
		

		Pipeline pipelineLogReg = new Pipeline().setStages(new PipelineStage[] { tokenizer, remover, hashingTF, cvLogReg});

		/*
		 * LOG-REG
		 */
		System.out.printf("\n\nLog-Reg\n");
		PipelineModel pipelineModelLogReg = pipelineLogReg.fit(trainSetSplit);
		Dataset<Row> predictedLogReg = pipelineModelLogReg.transform(testSetSplit);
		printConfusionMatrixEssence(predictedLogReg);
		System.out.printf("Area ROC Curve: %.4f\n", getAreaROCCurve(predictedLogReg));
		
		predictedLogReg.groupBy(col("target"),col("prediction")).count().show();

		/*
		 * Random Forest Classifier
		 */
		System.out.printf("\n\nRandom Forest Classifier\n");
		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol(label)
				.setFeaturesCol(vectorizer.getOutputCol()).setSeed(42);

		ParamMap[] paramGridRFC = new ParamGridBuilder().addGrid(rfc.maxDepth(), new int[] { 5, 7, 10 })
				.addGrid(rfc.numTrees(), new int[] { 40, 80 }).build();

		CrossValidator cvRFC = new CrossValidator().setEstimator(rfc)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridRFC).setNumFolds(5);
		
		Pipeline pipelineRFC = new Pipeline().setStages(new PipelineStage[] { tokenizer, remover, vectorizer, cvRFC});

		PipelineModel pipelineModelRFC = pipelineRFC.fit(trainSetSplit);

		Dataset<Row> predictedRFC = pipelineModelRFC.transform(testSetSplit);
		printConfusionMatrixEssence(predictedRFC);
		System.out.printf("Area ROC Curve: %.4f\n", getAreaROCCurve(predictedRFC));
		
		predictedLogReg.groupBy(col(label),col("prediction")).count().show();

	}
}