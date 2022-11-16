package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KaggleRegression {
	
	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();

	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).csv("src/main/resources/games.csv");
	}

	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		return dataset.drop(col("game_id")).drop(col("first")).drop(col("time_control_name"))
				.drop(col("game_end_reason")).drop(col("created_at")).drop(col("lexicon")).drop(col("rating_mode"));
	}

	private Dataset<Row> changeTypeColumns(Dataset<Row> dataset) {
		return dataset.withColumn("initial_time_seconds", dataset.col("initial_time_seconds").cast("Integer"))
				.withColumn("increment_seconds", dataset.col("increment_seconds").cast("Integer"))
				.withColumn("max_overtime_minutes", dataset.col("max_overtime_minutes").cast("Integer"))
				.withColumn("game_duration_seconds", dataset.col("game_duration_seconds").cast("Integer"));
	}

	private StringIndexer getStringIndexer() {
		return new StringIndexer()
				.setInputCol("winner")
				.setOutputCol("ind_winner");
	}

	private VectorAssembler getAssembler() {
		return new VectorAssembler().setInputCols(new String[] { 
				"initial_time_seconds", "increment_seconds", "max_overtime_minutes", "game_duration_seconds", "ind_winner" 
				}).setOutputCol("features");
	}

	private MinMaxScaler minmaxScaling() {
		return new MinMaxScaler()
				.setInputCol("features")
				.setOutputCol("scaledFeatures");
	}

	private Dataset<Row>[] splitSets(Dataset<Row> dataset, double[] proportie) {
		return dataset.randomSplit(proportie);
	}
	
	private LinearRegression getLinearRegModel() {
		return new LinearRegression()
				.setFeaturesCol("features")
				.setLabelCol("game_duration_seconds");
	}
	
	
	private void printSummaries(LinearRegressionSummary linregsum) {
		System.out.println("Trainingset --> RMSE: " + linregsum.rootMeanSquaredError());
		System.out.println("Testset --> RMSE: " + linregsum.rootMeanSquaredError());
		System.out.println("Trainingset --> r2: " + linregsum.r2());
		System.out.println("Testset --> r2: " + linregsum.r2());
	}
	
	private void printCorrelation(Dataset<Row> dataset) {
		Row r1 = Correlation.corr(dataset, "features").head();
		System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

		Row r2 = Correlation.corr(dataset, "features", "spearman").head();
		System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
	}


	public static void main(String[] args) {

		KaggleRegression kagglereg = new KaggleRegression();
		
		kagglereg.spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> regressiondata = kagglereg.getData();
		
		regressiondata = kagglereg.dropColumns(regressiondata);
		regressiondata = kagglereg.changeTypeColumns(regressiondata);
		
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {
				kagglereg.getStringIndexer(),
				kagglereg.getAssembler(),
				//kagglereg.getMinMaxScaler(),
				kagglereg.getLinearRegModel()
				});
		
		double[] verhouding = {0.8,0.2};
		Dataset<Row>[] sets = kagglereg.splitSets(regressiondata, verhouding);
		PipelineModel model = pipeline.fit(sets[0]);
		Dataset<Row> predictions = model.transform(sets[1]);
		
		predictions.show();
		kagglereg.printCorrelation(predictions);
		
		/*
		 * RMSE
		 */

	}

}
