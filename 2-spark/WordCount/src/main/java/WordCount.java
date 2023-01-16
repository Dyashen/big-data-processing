import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lower;

public class WordCount {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
				.appName("WordCount")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> contents = spark.read().text("src/main/resources/contents.txt");
		
		contents = contents
				.withColumn("woorden", explode(split(col("value"), " ")))
				.drop("value")
				.groupBy(col("woorden"))
				.count()
				.orderBy(desc("count"));
		
		contents.write()
				.mode(SaveMode.Overwrite)
				.csv("src/main/resources/contents-output");
		
		spark.close();

	}

}
