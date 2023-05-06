package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.Arrays;
//import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import breeze.optimize.linear.LinearProgram.Result;

public class ViewingFiguresSolution {

	private static SparkSession spark;

	public static void main(String[] args) {

		if (args.length != 0 && args.length != 4) {
			System.err.println("Usage ViewingFiguresExercise [<views> <chapters> <titles> <output>]");
			System.exit(-1);
		}

		boolean testMode = args.length == 0;

		if (testMode) {
			spark = SparkSession.builder().appName("ViewingFigures").master("local[*]").getOrCreate();
		} else {
			spark = SparkSession.builder().appName("ViewingFigures").getOrCreate();
		}

		UDF1<Double, Integer> score = new UDF1<Double, Integer>() {
			public Integer call(Double percent) throws Exception {
				if (percent > 0.9) {
					return 10;
				} else if (percent > 0.5) {
					return 6;
				} else if (percent > 0.25) {
					return 2;
				} else {
					return 0;
				}
			}
		};

		final String viewPath = testMode ? null : args[0];
		final String chapterPath = testMode ? null : args[1];
		final String titlePath = testMode ? null : args[2];

		/*
		 * Dataframes ophalen
		 */
		Dataset<Row> viewDF = readViews(viewPath, testMode);
		Dataset<Row> chaptersDF = readChapters(chapterPath, testMode);
		Dataset<Row> titleDF = readTitles(titlePath, testMode);

		viewDF.show();
		chaptersDF.show();
		titleDF.show();

		/*
		 * Totaal per chapter berekenen
		 */
		Dataset<Row> chaptersPerCourse = chaptersDF.drop("chapterId").groupBy("courseId").count()
				.withColumnRenamed("count", "chapters");
		chaptersPerCourse.show();

		viewDF = viewDF.distinct();
		viewDF.show();

		/*
		 * percent berekenen
		 */
		Dataset<Row> percentageDF = viewDF
				.join(chaptersDF, "chapterId")
				.drop("chapterId")
				.groupBy("userId", "courseId")
				.count()
				.drop("userId")
				.join(chaptersPerCourse, "courseId")
				.withColumn("percentage", col("count").divide(col("chapters")))
				.drop("chapters","count");
		
		percentageDF.show();
		
		/*
		 * UDF aanspreken
		 */
		spark.udf().register("berekenScore", score, DataTypes.IntegerType);
		
		Dataset<Row> result = percentageDF
				.withColumn("score",  call_udf("berekenScore",col("percentage")))
				.drop("percentage")
				.groupBy("courseId").agg(sum("score").as("total"))
				.join(titleDF, "courseId")
				.orderBy(desc("total"));
		
		if(!testMode) {
			final String outpString = args[3];
			result = result.repartition(1); // bij meerdere executors zal alle data op verschillende plaatsen zijn. Doe dit om alles op één plaats te plaatsen.
			result.write().csv(outpString);
		}
		


		spark.close();
	}

	private static Dataset<Row> readViews(String viewPath, boolean testmode) {

		java.util.List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("userId", DataTypes.IntegerType, false),
				DataTypes.createStructField("chapterId", DataTypes.IntegerType, false));

		StructType schema = DataTypes.createStructType(fields);

		if (testmode) {
			java.util.List<Row> rows = new ArrayList<>();
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

		java.util.List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("chapterId", DataTypes.IntegerType, false),
				DataTypes.createStructField("courseId", DataTypes.IntegerType, false));

		StructType schema = DataTypes.createStructType(fields);

		if (testmode) {
			java.util.List<Row> rows = new ArrayList<>();
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

	private static Dataset<Row> readTitles(String coursePath, boolean testmode) {

		java.util.List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("courseId", DataTypes.IntegerType, false),
				DataTypes.createStructField("title", DataTypes.StringType, false));

		StructType schema = DataTypes.createStructType(fields);

		if (testmode) {
			java.util.List<Row> rows = new ArrayList<>();
			rows.add(RowFactory.create(1, "Test 123"));
			rows.add(RowFactory.create(2, "Tweede test 132"));
			rows.add(RowFactory.create(3, "Derde test 134"));
			return spark.createDataFrame(rows, schema);
		} else {
			return spark.read().option("header", false).schema(schema).csv(coursePath);
		}
	}
}
