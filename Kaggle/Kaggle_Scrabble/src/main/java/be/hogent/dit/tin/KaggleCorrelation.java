package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.stat.Correlation;

public class KaggleCorrelation {
	
	SparkSession spark = SparkSession.builder().appName("KaggleLinearRegression").master("local[*]").getOrCreate();
	
	private Dataset<Row> getData() {
		return this.spark.read().option("header", true).csv("src/main/resources/games.csv");
	}

	private Dataset<Row> dropColumns(Dataset<Row> dataset) {
		return dataset
				.drop(col("game_id"))
				.drop(col("first"))
				.drop(col("time_control_name"))
				.drop(col("game_end_reason"))
				.drop(col("created_at"))
				.drop(col("lexicon"))
				.drop(col("rating_mode"));
	}

	public static void main(String[] args) {

		KaggleCorrelation kc = new KaggleCorrelation();
		
		Dataset<Row> dataset = kc.getData();
		
		Row r1 = Correlation.corr(dataset, "features").head();
		System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

		Row r2 = Correlation.corr(dataset, "features", "spearman").head();
		System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
		

	}

}
