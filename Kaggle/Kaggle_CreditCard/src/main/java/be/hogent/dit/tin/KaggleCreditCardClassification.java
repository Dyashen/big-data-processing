package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.rand;

import org.apache.spark.ml.Pipeline;
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
import org.apache.spark.sql.SparkSession;

public class KaggleCreditCardClassification {

	private static final String label = "Class";
	private static final String prediction = "prediction";
	private static final double[] verhouding = { 0.8, 0.2 };

	static SparkSession spark = SparkSession.builder().appName("CreditCardClassification").master("local[*]")
			.getOrCreate();

	private static Dataset<Row> getData() {
		return spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/creditcard.csv");
	}

	private static Dataset<Row> createSubSample(Dataset<Row> dataset) {
		dataset = dataset.sample(1.0);
		dataset = dataset.orderBy(rand());
		Dataset<Row> nietFraudulent = dataset.where(col(label).equalTo(1));
		long lengte = nietFraudulent.count();
		Dataset<Row> fraudulent = dataset.where(col(label).equalTo(0)).limit((int) lengte);
		dataset = nietFraudulent.union(fraudulent);
		return dataset.sample(1.0);
	}

	private static double getAreaROCCurve(Dataset<Row> predictions) {
		return new BinaryClassificationEvaluator().setLabelCol(label).evaluate(predictions);
	}

	private static void printCorrelation(Dataset<Row> dataset) {
		Row r1 = Correlation.corr(dataset, "pcaFeatures").head();
		System.out.printf("\n\nCorrelation Matrix\n");
		Matrix matrix = r1.getAs(0);
		for (int i = 0; i < matrix.numRows(); i++) {
			for (int j = 0; j < matrix.numCols(); j++) {
				System.out.printf("%.2f \t", matrix.apply(i, j));
			}
			System.out.println();
		}
	}

	private static void printConfusionMatrixMetrics(Dataset<Row> predictions_and_labels) {
		Dataset<Row> preds_and_labels = predictions_and_labels.select(prediction, label).orderBy(prediction)
				.withColumn(label, col(label).cast("double"));

		MulticlassMetrics metrics = new MulticlassMetrics(preds_and_labels);
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
		System.out.printf("Recall: %.5f \n", metrics.accuracy());
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> data = getData();

		data.show();
		data.groupBy(label).count().show();

		// Sub-sample maken
		data = createSubSample(data);
		data.groupBy(label).count().show();

		/*
		 * Alle features opslaan in een String array. Alle nodige features beginnen met
		 * een V-teken. Strings hebben een startswith-methode waarmee we kunnen
		 * filteren.
		 */
		String[] arrFeatures = new String[28];
		int teller = 0;
		for (String kolom : data.columns()) {
			if (kolom.startsWith("V")) {
				arrFeatures[teller] = kolom;
				teller++;
			}
		}

		/*
		 * De transformers: Assembler zal de features (meerdere kolommen) omzetten naar
		 * één kolom met daarin een vector van features. MinMax zal de features gaan
		 * scalen van waarde 0 tot en met waarde 1. PCA zal dimension reduction
		 * uitvoeren. Hiermee gaan we proberen om enkel de primaire features te behouden
		 * zonder side-effects in de resultaten.
		 */
		VectorAssembler assembler = new VectorAssembler().setInputCols(arrFeatures).setOutputCol("features");
		MinMaxScaler minmax = new MinMaxScaler().setMax(1.0).setMin(0.0).setInputCol(assembler.getOutputCol())
				.setOutputCol("scaledFeatures");
		PCA pca = new PCA().setInputCol(minmax.getOutputCol()).setOutputCol("pcaFeatures").setK(3);

		Dataset<Row>[] datasets = data.randomSplit(verhouding);

		Dataset<Row> trainSetSplit = datasets[0];
		Dataset<Row> trainTestSplit = datasets[1];

		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol(label).setFeaturesCol(pca.getOutputCol());

		Pipeline pipelineRFC = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, rfc });

		ParamMap[] paramGridRFC = new ParamGridBuilder().addGrid(rfc.maxDepth(), new int[] { 3, 7, 9 })
				.addGrid(rfc.numTrees(), new int[] { 40, 60, 80 }).addGrid(pca.k(), new int[] { 3, 6, 9 }).build();

		/*
		 * 1. Crossvalidatie zal het paramgrid testen om de meest optimale resultaten te
		 * bekomen. Dit wordt bepaald door een meegegeven metriek. 2. Het model trainen
		 * met de trainingset.
		 */
		CrossValidator cvRFC = new CrossValidator().setEstimator(pipelineRFC)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridRFC);
		CrossValidatorModel cvmRFC = cvRFC.fit(trainSetSplit);

		System.out.printf("Beste model: %s\n", cvmRFC.bestModel().params().toString());

		Dataset<Row> predictionsRFC = cvmRFC.transform(trainTestSplit);

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

		/*
		 * Volgorde pipeline: 1. Features omzetten naar één kolom. 2. Features scalen
		 * naar range 0 t.e.m. 1 3. Dimensionality reduction (dit moet na de scaler
		 * gebeuren!) 4. LinearSVM model.
		 */
		Pipeline pipelineSVM = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, rfc });

		ParamMap[] paramGridSVM = new ParamGridBuilder().addGrid(svc.maxIter(), new int[] { 5, 10, 15 })
				.addGrid(svc.regParam(), new double[] { 0.1, 0.2, 0.3 }).addGrid(pca.k(), new int[] { 3, 6, 9 })
				.build();

		CrossValidator cvSVM = new CrossValidator().setEstimator(pipelineSVM)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol(label))
				.setEstimatorParamMaps(paramGridSVM);

		CrossValidatorModel cvmSVM = cvSVM.fit(trainSetSplit);

		System.out.printf("Beste parameters: %s\n", cvmSVM.bestModel().params().toString());

		Dataset<Row> predictionsSVM = cvmSVM.transform(trainTestSplit);

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
				.setEstimatorParamMaps(paramGridLogReg);

		CrossValidatorModel cvmLogReg = cvLogReg.fit(trainSetSplit);

		System.out.printf("Beste model: %s\n", cvmLogReg.bestModel().params().toString());

		Dataset<Row> predictionsLogReg = cvmLogReg.transform(trainTestSplit);

		System.out.printf("\n\nLogistische Regressie\n");
		printConfusionMatrixMetrics(predictionsLogReg);
		System.out.printf("Area ROC curve: %.4f\n", getAreaROCCurve(predictionsLogReg));
		predictionsRFC.groupBy(col(label), col(prediction)).count().show(); // confusion matrix

	}
}
