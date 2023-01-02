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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DisasterTweetClassification {

	private static final double[] verhouding = { 0.8, 0.2 };
	private static final String label = "target";
	private static final String prediction = "prediction";

	private static SparkSession spark = SparkSession.builder().appName("DisasterTweet").master("local[1]")
			.getOrCreate();

	/*
	 * Een dataset opsplitsen naargelang een verhouding. De verhouding moeten twee
	 * doubles zijn: %training & %test.
	 */

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

	private static double getAreaROCCurve(Dataset<Row> dataframe) {
		return new BinaryClassificationEvaluator().setLabelCol(label).evaluate(dataframe);
	}

	private static void printConfusionMatrixEssence(Dataset<Row> dataframe) {
		System.out.println();
		dataframe = dataframe.select(prediction, label).orderBy(prediction)
				.withColumn("target_d", col(label).cast("double")).drop(col(label));

		MulticlassMetrics metrics = new MulticlassMetrics(dataframe);
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
		System.out.printf("Nauwkeurigheid: %.5f \n", metrics.accuracy());
	}

	private static void createSubmission(Dataset<Row> dataframe, String fnaam) {
		String output = "src/main/resources/" + fnaam + ".csv";
		dataframe = dataframe.select("id", "prediction");
		dataframe.write().mode(SaveMode.Overwrite).csv(output);
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> train = getTrainingData();
		Dataset<Row> test = getTestData();

		train.show();
		test.show();

		/*
		 * CLEANEN
		 */
		train = train.select(col("id"), col("text"), col(label));
		train = train.na().drop();
		train = train.withColumn("text", regexp_replace(col("text"), "\\d+", ""));

		test = test.select(col("id"), col("text"));
		test = test.na().drop();
		test = test.withColumn("text", regexp_replace(col("text"), "\\d+", ""));

		/*
		 * SUBSAMPLE MAKEN
		 */
		train = createSubSample(train);
		train.groupBy(label).count().show();

		Dataset<Row>[] datasets = train.randomSplit(verhouding, 42);
		Dataset<Row> trainSetSplit = datasets[0];
		Dataset<Row> trainTestSplit = datasets[1];

		System.out.printf("Lengte trainsetsplit: %s\n", trainSetSplit.count());
		System.out.printf("Lengte testsetsplit: %s\n", trainTestSplit.count());

		/*
		 * PIPELINE
		 */
		RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("words").setPattern("\\W");

		StopWordsRemover remover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol())
				.setOutputCol("filtered");

		HashingTF hashingTF = new HashingTF().setInputCol(remover.getOutputCol()).setOutputCol("hashedFeatures");

		LogisticRegression lr = new LogisticRegression().setFeaturesCol(hashingTF.getOutputCol()).setLabelCol(label)
				.setMaxIter(10).setRegParam(0.001);

		ParamMap[] paramGridLogReg = new ParamGridBuilder().addGrid(lr.maxIter(), new int[] { 30, 40, 50 })
				.addGrid(lr.threshold(), new double[] { 0.8, 0.85 }).build();

		CrossValidator cvLogReg = new CrossValidator().setEstimator(lr)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridLogReg);

		Pipeline pipelineLogReg = new Pipeline()
				.setStages(new PipelineStage[] { tokenizer, remover, hashingTF, cvLogReg });

		/*
		 * LOG-REG
		 */
		System.out.printf("\n\nLog-Reg\n");
		PipelineModel pipelineModelLogReg = pipelineLogReg.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nMax Iter: %s\nReg params: %s",
				cvLogReg.getEstimatorParamMaps()[0].get(lr.maxIter()).toString(),
				cvLogReg.getEstimatorParamMaps()[1].get(lr.threshold()).toString());

		Dataset<Row> predictedLogReg = pipelineModelLogReg.transform(trainTestSplit);
		printConfusionMatrixEssence(predictedLogReg);
		System.out.printf("Area ROC Curve: %.4f\n", getAreaROCCurve(predictedLogReg));

		// voorspellingen
		predictedLogReg.groupBy(col(label), col(prediction)).count().show();
		createSubmission(predictedLogReg, "submissionLogReg");

		/*
		 * Random Forest Classifier
		 */
		System.out.printf("\n\nRandom Forest Classifier\n");

		CountVectorizer vectorizer = new CountVectorizer().setInputCol(remover.getOutputCol()).setOutputCol("features")
				.setVocabSize(10000).setMinDF(5);

		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol(label)
				.setFeaturesCol(vectorizer.getOutputCol()).setSeed(42);

		ParamMap[] paramGridRFC = new ParamGridBuilder().addGrid(rfc.maxDepth(), new int[] { 2, 4, 5 })
				.addGrid(rfc.numTrees(), new int[] { 20, 30, 40 }).build();

		CrossValidator cvRFC = new CrossValidator().setEstimator(rfc)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridRFC).setNumFolds(5);

		Pipeline pipelineRFC = new Pipeline().setStages(new PipelineStage[] { tokenizer, remover, vectorizer, cvRFC });

		PipelineModel pipelineModelRFC = pipelineRFC.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nMax depth: %s\nNumber of trees: %s",
				cvRFC.getEstimatorParamMaps()[0].get(rfc.maxDepth()).toString(),
				cvRFC.getEstimatorParamMaps()[1].get(rfc.numTrees()).toString());

		Dataset<Row> predictedRFC = pipelineModelRFC.transform(trainTestSplit);
		printConfusionMatrixEssence(predictedRFC);
		System.out.printf("Area ROC Curve: %.4f\n", getAreaROCCurve(predictedRFC));

		predictedRFC.groupBy(col(label), col("prediction")).count().show();
		createSubmission(predictedRFC, "submissionRFC");

	}
}