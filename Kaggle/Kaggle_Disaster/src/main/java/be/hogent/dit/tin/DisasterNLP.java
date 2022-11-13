package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DisasterNLP {
	
	SparkSession spark = SparkSession
			.builder()
			.appName("KaggleLinearRegression")
			.master("local[*]")
			.getOrCreate();
	
	private Dataset<Row> getTrainingData(){
		return this.spark
				.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/train.csv");
	}
	
	private Dataset<Row> getTestData(){
		return this.spark
				.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/test.csv");
	}
	
	
	private RegexTokenizer getRegexTokenizer(){
		return new RegexTokenizer()
				.setInputCol("str_only")
				.setOutputCol("words")
				.setPattern("\\W");
	}
	
	
	private StopWordsRemover getStopWordsRemover() {
		return new StopWordsRemover()
				.setInputCol("words")
				.setOutputCol("filtered");
	}
	
	
	private CountVectorizer getCountVectorizer(){
		return new CountVectorizer()
				.setInputCol("filtered")
				.setOutputCol("features");
	}
	
	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] verhouding) {
		return dataset.randomSplit(verhouding);
	}

	public static void main(String[] args) {
		
		DisasterNLP nlp = new DisasterNLP(); 
		
		Dataset<Row> training = nlp.getTrainingData();
		Dataset<Row> test = nlp.getTestData();
		
		
		training.groupBy(col("target")).count().show();
		
		
		Dataset<Row> ml_df = training.select(col("id"), col("text"), col("target"));
		
		ml_df = ml_df.na().drop();
		
		
		// Geen getallen!
		ml_df = ml_df.withColumn("str_only", regexp_replace(col("text"), "\\d+", ""));
		
		ml_df.show();
		
		
		// Enkel Strings
		RegexTokenizer regextokenizer = new RegexTokenizer()
				.setInputCol("str_only")
				.setOutputCol("words")
				.setPattern("\\W");
		
		Dataset<Row> raw_words = regextokenizer.transform(ml_df);
		
		
		// Geen stopwoorden!
		StopWordsRemover remover = new StopWordsRemover()
				.setInputCol("words")
				.setOutputCol("filtered");
		
		
		Dataset<Row> words_df = remover.transform(raw_words);
		
		CountVectorizer cv = new CountVectorizer()
				.setInputCol("filtered")
				.setOutputCol("features");
		
		CountVectorizerModel model = cv.fit(words_df);
		
		Dataset<Row> countVectorizer_train = model.transform(words_df);
		countVectorizer_train = model.transform(words_df);
		countVectorizer_train.show();
		
		countVectorizer_train.select("text","words","filtered","features","target").show();
		
		/* Dataframe opsplitsen in twee delen. */
		double[] verhouding = {0.8,0.2};
		Dataset<Row>[] sets = nlp.splitSets(countVectorizer_train, verhouding);
		
		
		Dataset<Row> trainData = countVectorizer_train;
		
		
		
		
		
//		
//		Dataset<Row> testData = test.select("id","text");
//		testData = testData.withColumn("only_str", regexp_replace(col("text"), "\\d+", ""));
//		RegexTokenizer regex_tokenizer = new RegexTokenizer()
//				.setInputCol("only_str")
//				.setOutputCol("words")
//				.setPattern("\\W");
//		
//		testData = regex_tokenizer.transform(testData);
//		
//		StopWordsRemover removerTest = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
//		
//		testData = removerTest.transform(testData);
//		
//		CountVectorizer cvTest = new CountVectorizer()
//				.setInputCol("filtered")
//				.setOutputCol("features");
//		CountVectorizerModel modelTest = cv.fit(words_df);
//		Dataset<Row> countVectorizer_test = model.transform(testData);
//		testData = countVectorizer_test;
//		
//		testData.show();
//		
		
		/*
		 * LOGREG: Ruimte onder de ROC-curve
		 */
		
		LogisticRegression lr = new LogisticRegression()
				.setFeaturesCol("features")
				.setLabelCol("target")
				.setMaxIter(10);
		
		LogisticRegressionModel lrModel = lr.fit(sets[0]);
		
		Dataset<Row> lrPredictions = lrModel.transform(sets[1]);
		
		BinaryClassificationEvaluator lrEval = new BinaryClassificationEvaluator()
				.setLabelCol("target");
		
		System.out.printf("Ruimte onder de ROC-curve: %.5f \n", lrEval.evaluate(lrPredictions));
		
		/*
		 * LOGREG: Nauwkeurigheid
		 */
		
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("target")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");
		
		double lr_accuracy = evaluator.evaluate(lrPredictions);
		
		System.out.printf("Nauwkeurigheid van het LogReg model: %.5f \n", lr_accuracy);
		
		
		/*
		 * DECISION TREE
		 */
		
		
//		NaiveBayes nb = new NaiveBayes()
//				.setModelType("multinomial")
//				.setLabelCol("target")
//				.setFeaturesCol("features");
//		
//		NaiveBayesModel nbModel = nb.fit(trainData);
//		
//		Dataset<Row> nb_predictions = nbModel.transform(sets[1]);
		

	}

}
