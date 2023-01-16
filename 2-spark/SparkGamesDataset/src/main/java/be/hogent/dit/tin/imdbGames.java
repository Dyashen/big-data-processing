package be.hogent.dit.tin;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.year;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_union;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.count;

import java.util.Arrays;

public class imdbGames {
	
	private final static String inputPath = "src/main/resources/imdb-videogames.csv";
	
	static SparkSession spark = SparkSession.builder().appName("CreditCardClassification").master("local[*]")
			.getOrCreate();
	
	private static Dataset<Row> getData() {
		
		java.util.List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("name", DataTypes.StringType, false),
				DataTypes.createStructField("url", DataTypes.StringType, false),
				DataTypes.createStructField("year", DataTypes.IntegerType, false),
				DataTypes.createStructField("certificate", DataTypes.StringType, false),
				DataTypes.createStructField("rating", DataTypes.DoubleType, false),
				DataTypes.createStructField("votes", DataTypes.IntegerType, false),
				DataTypes.createStructField("plot", DataTypes.StringType, false),
				DataTypes.createStructField("Action", DataTypes.BooleanType, false),
				DataTypes.createStructField("Adventure", DataTypes.BooleanType, false),
				DataTypes.createStructField("Comedy", DataTypes.BooleanType, false),
				DataTypes.createStructField("Crime", DataTypes.BooleanType, false),
				DataTypes.createStructField("Family", DataTypes.BooleanType, false),
				DataTypes.createStructField("Fantasy", DataTypes.BooleanType, false),
				DataTypes.createStructField("Mystery", DataTypes.BooleanType, false),
				DataTypes.createStructField("Scifi", DataTypes.BooleanType, false),
				DataTypes.createStructField("Thriller", DataTypes.BooleanType, false));

		StructType schema = DataTypes.createStructType(fields);
		
		return spark.read()
					.option("header", true)
					.schema(schema)
					.csv(inputPath);
	}

	public static void main(String[] args) {
		
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> data = getData();
		
		data = data.drop(col("url"));
		
		data = data.na().drop();
		
		// aantal per jaar tonen
		data.groupBy(col("year"))
			.count().alias("AantalPerJaar")
			.orderBy(desc("year"))
			.show(false);
		
		// rating per jaar tonen
		data.groupBy(col("year"))
			.avg("rating").alias("gemiddeldeScorePerJaar")
			.orderBy(desc("year"))
			.show(false);
		
		// alle games die nog moeten uitkomen
		Dataset<Row> upcomingGames = data.where(col("year").geq(year(current_date())));
		upcomingGames.show(false);
		
		
		String[] genres = new String[] {"Adventure","Comedy","Crime","Family","Fantasy","Mystery","Scifi","Thriller"};
		Column emptyArray = array();
		
		for (String genre : genres) {
			
			Column keyArray = array(lit(genre));

			Column concatenated = when(col(genre)
					.equalTo("true"), array_union(emptyArray, keyArray))
					.otherwise(emptyArray);
		    emptyArray = concatenated;
		}
		
		data = data.withColumn("genres", emptyArray)
			.select("name","year","certificate","rating","votes","plot","genres")
			//.show()
			;

		data.withColumn("element", explode(col("genres")))
			.groupBy("element")
			.count()
			.orderBy(desc("count"))
			.show();
		
		data.withColumn("element", explode(col("genres")))
			.groupBy("element")
			.avg("votes").as("gemiddeldAantalStemmen")
			.orderBy(desc("avg(votes)"))
			.show();
		
	}
}
