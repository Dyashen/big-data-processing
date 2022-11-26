package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.param.Param;
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

	static SparkSession spark = SparkSession.builder().appName("CreditCardClassification").master("local[*]").getOrCreate();

	private static Dataset<Row> getData() {
		return spark.read().option("header", true).option("inferSchema", true)
				.csv("src/main/resources/creditcard.csv");
	}
	
	private static Dataset<Row> createSubSample(Dataset<Row> dataset){
		dataset = dataset.sample(1.0);
		Dataset<Row> nietFraudulent = dataset.where(col("class").equalTo(1));
		long lengte = nietFraudulent.count();
		Dataset<Row> fraudulent = dataset.where(col("class").equalTo(0)).limit((int)lengte);
		dataset = nietFraudulent.union(fraudulent);
		return dataset.sample(1.0);
	}

	private static Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
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
		
	private static void printCorrelation(Dataset<Row> dataset) {
		Row r1 = Correlation.corr(dataset, "features").head();
		System.out.printf("\n\nCorrelation Matrix\n");
		Matrix matrix = r1.getAs(0);
		for (int i = 0; i < matrix.numRows(); i++) {
			for (int j = 0; j < matrix.numCols(); j++) {
				System.out.printf("%.2f \t", matrix.apply(i, j));
			}
			System.out.println();
		}
	}

	private static void printConfusionMatrixEssence(Dataset<Row> predictions_and_labels) {
		Dataset<Row> preds_and_labels = predictions_and_labels.select("prediction", "Class").orderBy("prediction")
				.withColumn("Class", col("Class").cast("double"));

		MulticlassMetrics metrics = new MulticlassMetrics(preds_and_labels);
		System.out.println(metrics.confusionMatrix());
		System.out.printf("Precision: %.5f \n", metrics.weightedPrecision());
		System.out.printf("Recall: %.5f \n", metrics.weightedRecall());
	}

	public static void main(String[] args) {

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> data = getData();
		
		/*
		 * Data zoals het is
		 */
		data.show();
		data.groupBy("class").count().show();
		
		/*
		 * Sub-sample maken met 50:50 ratio en grootte [492:492]
		 */
		data = createSubSample(data);
		data.groupBy("class").count().show();
		
		/*
		 * Alle features opslaan in een String array.
		 * Alle nodige features beginnen met een V-teken. 
		 * Strings hebben een startswith-methode waarmee we kunnen filteren.
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
		 * De transformers:
		 * Assembler zal de features (meerdere kolommen) omzetten naar één kolom met daarin een vector van features.
		 * MinMax zal de features gaan scalen van waarde 0 tot en met waarde 1.
		 * PCA zal dimension reduction uitvoeren. Hiermee gaan we proberen om enkel de primaire features te behouden zonder side-effects in de resultaten.
		 */
		VectorAssembler assembler = new VectorAssembler().setInputCols(arrFeatures).setOutputCol("features");
		MinMaxScaler minmax = new MinMaxScaler().setMax(1.0).setMin(0.0).setInputCol("features")
				.setOutputCol("scaledFeatures");
		PCA pca = new PCA().setInputCol("scaledFeatures").setOutputCol("pcaFeatures").setK(3);

		/*
		 * Het splitten van de dataset. Trainingset = 80% & Testset = 20%
		 */
		double[] verhouding = { 0.8, 0.2 };
		Dataset<Row>[] datasets = splitSets(data, verhouding);

		/*
		 * Random Forest Classifier
		 */
		RandomForestClassifier rfc = new RandomForestClassifier().setLabelCol("Class").setFeaturesCol("pcaFeatures");

		/*
		 * Volgorde pipeline:
		 * 1. Features omzetten naar één kolom.
		 * 2. Features scalen naar range 0 t.e.m. 1
		 * 3. Dimensionality reduction (dit moet na de scaler gebeuren!)
		 * 4. Random Forest Classifier model.
		 */
		Pipeline pipelineRFC = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, rfc });

		/*
		 * Parameters bepalen:
		 * 1. Starten met de standaardwaarden en omliggende waarden nemen (- +).
		 * 2. Afweging maken uit vorige testen (trial & error).
		 */
		
		
		ParamMap[] paramGridRFC = new ParamGridBuilder()
				.addGrid(rfc.maxDepth(), new int[] { 10, 20, 30 })
				.addGrid(rfc.numTrees(), new int[] { 40, 60, 80 })
				.addGrid(pca.k(), new int[] {3,6,9})
				.build();
		

		/*
		 * 1. Crossvalidatie zal het paramgrid testen om de meest optimale resultaten te bekomen. 
		 * Dit wordt bepaald door een meegegeven metriek. 
		 * 2. Het model trainen met de trainingset.
		 */
		CrossValidator cvRFC = new CrossValidator().setEstimator(pipelineRFC)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Class"))
				.setEstimatorParamMaps(paramGridRFC);
		CrossValidatorModel cvmRFC = cvRFC.fit(datasets[0]);

		/*
		 * We vragen van het crossvalidatiemodel op welke parameters de beste zijn.
		 */
		System.out.printf("");
		for(Param<?> p : cvmRFC.bestModel().params()) {
			System.out.println(p);
		}
		System.out.printf("Beste model: %s\n", cvmRFC.bestModel().params().toString());

		/*
		 * Het model maakt voorspellingen op de testset.
		 */
		Dataset<Row> predictionsRFC = cvmRFC.transform(datasets[1]);

		/*
		 * De confusion matrix wordt uitgeprint.
		 */
		System.out.printf("\n\nRandom Forest Classifier\n");
		printConfusionMatrixEssence(predictionsRFC);

		/*
		 * Lineaire SVM
		 */
		LinearSVC svc = new LinearSVC();
		/*
		 * Volgorde pipeline:
		 * 1. Features omzetten naar één kolom.
		 * 2. Features scalen naar range 0 t.e.m. 1
		 * 3. Dimensionality reduction (dit moet na de scaler gebeuren!)
		 * 4. LinearSVM model.
		 */
		Pipeline pipelineSVM = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, rfc });

		ParamMap[] paramGridSVM = new ParamGridBuilder()
				.addGrid(svc.maxIter(), new int[] { 5, 10, 15 })
				.addGrid(svc.regParam(), new double[] { 0.1, 0.2, 0.3 })
				.addGrid(pca.k(), new int[] {3,6,9})
				.build();

		CrossValidator cvSVM = new CrossValidator().setEstimator(pipelineSVM)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Class"))
				.setEstimatorParamMaps(paramGridSVM);

		CrossValidatorModel cvmSVM = cvSVM.fit(datasets[0]);
		
		System.out.printf("Beste parameters: %s\n", cvmSVM.bestModel().params().toString());

		Dataset<Row> predictionsSVM = cvmSVM.transform(datasets[1]);

		System.out.printf("\n\nLineaire SVM Classifier\n");
		printConfusionMatrixEssence(predictionsSVM);

		/*
		 * Logistische Regressie
		 */
		LogisticRegression lr = new LogisticRegression().setLabelCol("Class").setFeaturesCol("pcaFeatures");

		Pipeline pipelineLogReg = new Pipeline().setStages(new PipelineStage[] { assembler, minmax, pca, lr });

		ParamMap[] paramGridLogReg = new ParamGridBuilder()
				.addGrid(lr.threshold(), new double[] { 0.5, 0.75, 0.9 })
				.addGrid(lr.maxIter(), new int[] { 5, 10, 15 })
				.addGrid(pca.k(), new int[] {3,6,9})
				.build();

		CrossValidator cvLogReg = new CrossValidator().setEstimator(pipelineLogReg)
				.setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Class"))
				.setEstimatorParamMaps(paramGridLogReg);

		CrossValidatorModel cvmLogReg = cvLogReg.fit(datasets[0]);

		System.out.printf("Beste model: %s\n", cvmLogReg.bestModel().params().toString());

		Dataset<Row> predictionsLogReg = cvmLogReg.transform(datasets[1]);

		System.out.printf("\n\nLogistische Regressie\n");
		printConfusionMatrixEssence(predictionsLogReg);

	}
}
