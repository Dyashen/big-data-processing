package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KaggleCreditCardClassification {

	SparkSession spark = SparkSession.builder().appName("CreditCardClassification").master("local[*]").getOrCreate();

	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).option("inferSchema", true)
				.csv("src/main/resources/creditcard.csv");
	}

	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	private void printConfusionMatrixEssence(Dataset<Row> predictions_and_labels) {
		Dataset<Row> preds_and_labels = predictions_and_labels.select("prediction", "Class").orderBy("prediction")
				.withColumn("Class", col("Class").cast("double"));

		MulticlassMetrics metrics = new MulticlassMetrics(preds_and_labels);
		System.out.println(metrics.confusionMatrix());
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
	}

	public static void main(String[] args) {

		KaggleCreditCardClassification ccc = new KaggleCreditCardClassification();
		ccc.spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> data = ccc.getData();

		String[] arrFeatures = new String[28];
		int teller = 0;
		for (String kolom : data.columns()) {
			if (kolom.startsWith("V")) {
				arrFeatures[teller] = kolom;
				teller++;
			}
		}

		VectorAssembler assembler = new VectorAssembler().setInputCols(arrFeatures).setOutputCol("features");
		MinMaxScaler minmax = new MinMaxScaler().setMax(1.0).setMin(0.0).setInputCol("features")
				.setOutputCol("scaledFeatures");

		double[] verhouding = { 0.8, 0.2 };

		Dataset<Row>[] datasets = ccc.splitSets(data, verhouding);
		
		/*
		 * Random Forest Classifier
		 */
		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol("Class").setFeaturesCol("scaledFeatures");

		Pipeline pipelineRFC = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, rfc });

		ParamMap[] paramGridRFC = new ParamGridBuilder()
				.addGrid(rfc.maxDepth(), new int[] { 10, 20, 30 })
				.addGrid(rfc.numTrees(), new int[] { 40, 60, 80 })
				//.addGrid(rfc.thresholds(), new double[][] {})
				.build();

		CrossValidator cvRFC = new CrossValidator().setEstimator(pipelineRFC)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Class"))
				.setEstimatorParamMaps(paramGridRFC);

		CrossValidatorModel cvmRFC = cvRFC.fit(datasets[0]);

		System.out.printf("Beste model: %s\n", cvmRFC.bestModel().params().toString());

		Dataset<Row> predictionsRFC = cvmRFC.transform(datasets[1]);

		System.out.printf("\n\nRandom Forest Classifier\n");
		ccc.printConfusionMatrixEssence(predictionsRFC);
		
		/*
		 * Lineaire SVM
		 */
		LinearSVC svc = new LinearSVC();
		
		Pipeline pipelineSVM = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, rfc });

		ParamMap[] paramGridSVM = new ParamGridBuilder()
				.addGrid(svc.maxIter(), new int[] {5, 10, 15})
				.addGrid(svc.regParam(), new double[] {0.1, 0.2, 0.3})
				.build();

		CrossValidator cvSVM = new CrossValidator().setEstimator(pipelineSVM)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Class"))
				.setEstimatorParamMaps(paramGridSVM);
		
		System.out.printf("Beste parameters: %s\n", cvmRFC.bestModel().params().toString());
		
		CrossValidatorModel cvmSVM = cvSVM.fit(datasets[0]);

		Dataset<Row> predictionsSVM = cvmSVM.transform(datasets[1]);

		System.out.printf("\n\nLineaire SVM Classifier\n");
		ccc.printConfusionMatrixEssence(predictionsSVM);
		

		/*
		 * Logistische Regressie
		 */
		LogisticRegression lr = new LogisticRegression().setLabelCol("Class").setFeaturesCol("scaledFeatures");
		
		Pipeline pipelineLogReg = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, lr });
		
		ParamMap[] paramGridLogReg = new ParamGridBuilder()
				.addGrid(lr.threshold(), new double[] { 0.5, 0.75, 0.9 })
				.addGrid(lr.maxIter(), new int[] {5, 10, 15})
				.build();
		
		CrossValidator cvLogReg = new CrossValidator().setEstimator(pipelineLogReg)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Class"))
				.setEstimatorParamMaps(paramGridLogReg);

		CrossValidatorModel cvmLogReg = cvLogReg.fit(datasets[0]);

		System.out.printf("Beste model: %s\n", cvmLogReg.bestModel().params().toString());

		Dataset<Row> predictionsLogReg = cvmLogReg.transform(datasets[1]);

		System.out.printf("\n\nLogistische Regressie\n");
		ccc.printConfusionMatrixEssence(predictionsLogReg);
		
	}
}
