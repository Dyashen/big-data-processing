package be.hogent.dit.tin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class ViewingFiguresReport {
	
	static SparkSession spark;
	
	static final String INPUT_PATH_CHAPTERS = "";
	static final String INPUT_PATH_TITLES= "";
	static final String INPUT_PATH_VIEWS = "";
	static boolean testMode = true;
	static Integer USER_ID = 13;
	
	static UDF1<Double, Integer> udfBerekenScore = new UDF1<Double, Integer>() {
		
		public Integer call(Double t1) throws Exception {
			
			if (t1 >= 0.9) {
				return 10;
			} else if (t1 >= 0.5){
				return 4;
			} else if (t1 >= 0.25) {
				return 2;
			} else {
				return 0;
			}
		}
	};
	

	public static void main(String[] args) {
		
		if (testMode) {
			spark = SparkSession.builder().appName("ViewingFigures").master("local[*]").getOrCreate();
		} else {
			spark = SparkSession.builder().appName("ViewingFigures").getOrCreate();
			USER_ID = Integer.parseInt(args[0]);
		}
		
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> chapters = readChapters(INPUT_PATH_CHAPTERS, testMode);
		Dataset<Row> titles = readTitles(INPUT_PATH_TITLES, testMode);
		Dataset<Row> views = readViews(INPUT_PATH_VIEWS, testMode);
		
		chapters.show();
		titles.show();
		views.show();
		
		/*
		 * Aantal hoofdstukken per course.
		 */
		Dataset<Row> numberChaptersPerCourse = chapters
						.groupBy("courseId")
						.agg(count("chapterId").as("aantalVoorkomens"));
		numberChaptersPerCourse.show();
		
		/*
		 * Percentage berekenen
		 */
		Dataset<Row> viewsPercentage = views.join(chapters, "chapterId")
					.groupBy("userId", "courseId")
					.agg(count("chapterId").as("aantalBekeken"))
					.join(numberChaptersPerCourse, "courseId")
					.withColumn("percentage", col("aantalBekeken").divide(col("aantalVoorkomens")))
					.drop("aantalBekeken", "aantalVoorkomens");
		
		viewsPercentage.show();
		
		// UDF registreren
				
		spark.udf().register("berekenScore", udfBerekenScore, DataTypes.IntegerType);
		
		Dataset<Row> overview;
		
		viewsPercentage.join(titles, "courseId")
						.withColumn("total", call_udf("berekenScore", col("percentage")))
						.where(col("userId").equalTo(USER_ID))
						.drop("percentage", "userId")
						.show();
				
	}
	
	private static Dataset<Row> readTitles(String coursePath, boolean testmode) {

		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("courseId", DataTypes.IntegerType, false),
				DataTypes.createStructField("title", DataTypes.StringType, false));

		StructType schema = DataTypes.createStructType(fields);

		if (testmode) {
			List<Row> rows = new ArrayList<Row>();
			rows.add(RowFactory.create(1, "Test 123"));
			rows.add(RowFactory.create(2, "Tweede test 132"));
			rows.add(RowFactory.create(3, "Derde test 134"));
			return spark.createDataFrame(rows, schema);
		} else {
			return spark.read().option("header", false).schema(schema).csv(coursePath);
		}
	}
	
	private static Dataset<Row> readViews(String viewPath, boolean testmode) {

		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("userId", DataTypes.IntegerType, false),
				DataTypes.createStructField("chapterId", DataTypes.IntegerType, false));

		StructType schema = DataTypes.createStructType(fields);

		if (testmode) {
			List<Row> rows = new ArrayList<Row>();
			rows.add(RowFactory.create(14, 96));
			rows.add(RowFactory.create(14, 97));
			rows.add(RowFactory.create(13, 96));
			rows.add(RowFactory.create(13, 96));
			rows.add(RowFactory.create(13, 96));
			rows.add(RowFactory.create(14, 99));
			rows.add(RowFactory.create(13, 100));
			return spark.createDataFrame(rows, schema);
		} else {
			return spark.read().option("header", false).schema(schema).csv(viewPath);
		}
	}

	private static Dataset<Row> readChapters(String chapterPath, boolean testmode) {

		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("chapterId", DataTypes.IntegerType, false),
				DataTypes.createStructField("courseId", DataTypes.IntegerType, false));

		StructType schema = DataTypes.createStructType(fields);

		if (testmode) {
			List<Row> rows = new ArrayList<Row>();
			rows.add(RowFactory.create(96, 1));
			rows.add(RowFactory.create(97, 1));
			rows.add(RowFactory.create(98, 1));
			rows.add(RowFactory.create(99, 2));
			rows.add(RowFactory.create(100, 3));
			rows.add(RowFactory.create(101, 3));
			rows.add(RowFactory.create(102, 3));
			rows.add(RowFactory.create(103, 3));
			rows.add(RowFactory.create(104, 3));
			rows.add(RowFactory.create(105, 3));
			rows.add(RowFactory.create(106, 3));
			rows.add(RowFactory.create(107, 3));
			rows.add(RowFactory.create(108, 3));
			rows.add(RowFactory.create(109, 3));
			return spark.createDataFrame(rows, schema);

		} else {
			return spark.read().option("header", false).schema(schema).csv(chapterPath);
		}
	}
}
