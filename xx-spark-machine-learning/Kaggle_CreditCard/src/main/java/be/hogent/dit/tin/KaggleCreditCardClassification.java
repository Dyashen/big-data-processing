package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.rand;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class KaggleCreditCardClassification {

	private static final String label = "Class";
	private static final String prediction = "prediction";
	private static final double[] verhouding = { 0.8, 0.2 };

	static SparkSession spark = SparkSession.builder().appName("CreditCardClassification").master("local[*]")
			.getOrCreate();

	// data ophalen uit csv
	private static Dataset<Row> getData() {
		return spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/creditcard.csv");
	}

	// skewed data vermijden
	private static Dataset<Row> createSubSample(Dataset<Row> dataframe) {
		dataframe = dataframe.sample(1.0);
		dataframe = dataframe.orderBy(rand());
		Dataset<Row> nietFraudulent = dataframe.where(col(label).equalTo(1));
		long lengte = nietFraudulent.count();
		Dataset<Row> fraudulent = dataframe.where(col(label).equalTo(0)).limit((int) lengte);
		dataframe = nietFraudulent.union(fraudulent);
		return dataframe.sample(1.0);
	}

	private static double getAreaROCCurve(Dataset<Row> dataframe) {
		return new BinaryClassificationEvaluator().setLabelCol(label).evaluate(dataframe);
	}

	private static void printCorrelation(Dataset<Row> dataframe) {
		Row r1 = Correlation.corr(dataframe, "pcaFeatures").head();
		System.out.printf("\n\nCorrelation Matrix\n");
		Matrix matrix = r1.getAs(0);
		for (int i = 0; i < matrix.numRows(); i++) {
			for (int j = 0; j < matrix.numCols(); j++) {
				System.out.printf("%.2f \t", matrix.apply(i, j));
			}
			System.out.println();
		}
	}

	private static void printConfusionMatrixMetrics(Dataset<Row> dataframe) {
		dataframe = dataframe.select(prediction, label).orderBy(prediction).withColumn(label,
				col(label).cast("double"));

		MulticlassMetrics metrics = new MulticlassMetrics(dataframe);
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
		System.out.printf("Recall: %.5f \n", metrics.accuracy());
	}

	private static void createSubmission(Dataset<Row> dataframe, String fnaam) {
		String output = "src/main/resources/" + fnaam + ".csv";
		dataframe = dataframe.select("prediction");
		dataframe.write().mode(SaveMode.Overwrite).csv(output);
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> creditCardDataset = getData();

		// Hoeveel rijen per klasse? --> skewed
		creditCardDataset.show();
		creditCardDataset.groupBy(label).count().show();

		// Sub-sample maken
		Dataset<Row> creditCardSubSample = createSubSample(creditCardDataset);
		creditCardSubSample.groupBy(label).count().show();

		// Alle features toevoegen aan een array --> omzetten naar ��n vector.
		String[] arrFeatures = new String[28];
		int teller = 0;
		for (String kolom : creditCardSubSample.columns()) {
			if (kolom.startsWith("V")) {
				arrFeatures[teller] = kolom;
				teller++;
			}
		}

		// Transformers aanmaken
		VectorAssembler assembler = new VectorAssembler().setInputCols(arrFeatures).setOutputCol("features");
		MinMaxScaler minmax = new MinMaxScaler().setMax(1.0).setMin(0.0).setInputCol(assembler.getOutputCol())
				.setOutputCol("scaledFeatures");
		PCA pca = new PCA().setInputCol(minmax.getOutputCol()).setOutputCol("pcaFeatures").setK(3);

		// Dataset splitsen in twee delen: train- en testdeel.
		Dataset<Row>[] datasets = creditCardSubSample.randomSplit(verhouding);
		Dataset<Row> trainSetSplit = datasets[0];
		Dataset<Row> trainTestSplit = datasets[1];

		// RFC model
		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol(label).setFeaturesCol(pca.getOutputCol());

		ParamMap[] paramGridRFC = new ParamGridBuilder().addGrid(rfc.maxDepth(), new int[] { 3, 7, 9 })
				.addGrid(rfc.numTrees(), new int[] { 40, 60, 80 }).addGrid(pca.k(), new int[] { 3, 6, 9 }).build();

		CrossValidator cvRFC = new CrossValidator().setEstimator(rfc)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridRFC).setNumFolds(3);

		Pipeline pipelineRFC = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, cvRFC });

		PipelineModel pipelineModelRFC = pipelineRFC.fit(trainSetSplit);

		// ideale parameters uitprinten
		System.out.printf("Ideale parameters:\nMax depth: %s\nNum trees: %s\nk: %s",
				cvRFC.getEstimatorParamMaps()[0].get(rfc.maxDepth()).toString(),
				cvRFC.getEstimatorParamMaps()[1].get(rfc.numTrees()).toString(),
				cvRFC.getEstimatorParamMaps()[2].get(pca.k()).toString());

		// voorspellingen maken + uitschrijven naar csv
		Dataset<Row> predictionsRFC = pipelineModelRFC.transform(trainTestSplit);
		createSubmission(predictionsRFC, "submissionRFC");

		// Evaluatie
		System.out.printf("\n\nRandom Forest Classifier\n");
		printConfusionMatrixMetrics(predictionsRFC);
		printCorrelation(predictionsRFC);
		System.out.printf("Area ROC curve: %.4f\n", getAreaROCCurve(predictionsRFC));
		predictionsRFC.groupBy(col(label), col(prediction)).count().show(); // confusion matrix

		/*
		 * Lineaire SVM
		 */
		LinearSVC svc = new LinearSVC();

		Pipeline pipelineSVM = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, rfc });

		ParamMap[] paramGridSVM = new ParamGridBuilder().addGrid(svc.maxIter(), new int[] { 5, 10, 15 })
				.addGrid(svc.regParam(), new double[] { 0.1, 0.2, 0.3 }).addGrid(pca.k(), new int[] { 3, 6, 9 })
				.build();

		CrossValidator cvSVM = new CrossValidator().setEstimator(pipelineSVM)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridSVM).setNumFolds(3);

		CrossValidatorModel cvmSVM = cvSVM.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nMax Iter: %s\nReg params: %s\nk: %s",
				cvSVM.getEstimatorParamMaps()[0].get(svc.maxIter()).toString(),
				cvSVM.getEstimatorParamMaps()[1].get(svc.regParam()).toString(),
				cvSVM.getEstimatorParamMaps()[2].get(pca.k()).toString());

		// voorspellingen maken en uitschrijven naar csv
		Dataset<Row> predictionsSVM = cvmSVM.transform(trainTestSplit);
		createSubmission(predictionsSVM, "submissionSVM");

		System.out.printf("\n\nLineaire SVM Classifier\n");
		printConfusionMatrixMetrics(predictionsSVM);
		System.out.printf("Area ROC curve: %.4f\n", getAreaROCCurve(predictionsSVM));
		predictionsRFC.groupBy(col(label), col(prediction)).count().show(); // confusion matrix

		/*
		 * Logistische Regressie
		 */
		LogisticRegression lr = new LogisticRegression().setLabelCol(label).setFeaturesCol("pcaFeatures");

		Pipeline pipelineLogReg = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, lr });

		ParamMap[] paramGridLogReg = new ParamGridBuilder().addGrid(lr.threshold(), new double[] { 0.75, 0.9 })
				.addGrid(lr.maxIter(), new int[] { 5, 10, 15 }).addGrid(pca.k(), new int[] { 3, 6, 9 }).build();

		CrossValidator cvLogReg = new CrossValidator().setEstimator(pipelineLogReg)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridLogReg).setNumFolds(3);

		CrossValidatorModel cvmLogReg = cvLogReg.fit(trainSetSplit);

		System.out.printf("Ideale parameters:\nThreshold: %s\nMax Iterations: %s\nk: %s",
				cvLogReg.getEstimatorParamMaps()[0].get(lr.threshold()).toString(),
				cvLogReg.getEstimatorParamMaps()[1].get(lr.maxIter()).toString(),
				cvLogReg.getEstimatorParamMaps()[2].get(pca.k()).toString());

		Dataset<Row> predictionsLogReg = cvmLogReg.transform(trainTestSplit);
		createSubmission(predictionsLogReg, "submissionLR");

		System.out.printf("\n\nLogistische Regressie\n");
		printConfusionMatrixMetrics(predictionsLogReg);
		System.out.printf("Area ROC curve: %.4f\n", getAreaROCCurve(predictionsLogReg));
		predictionsRFC.groupBy(col(label), col(prediction)).count().show(); // confusion matrix
	}
}
