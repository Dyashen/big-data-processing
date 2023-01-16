package be.hogent.dit.tin;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ViewingFiguresReport {
	
	private static SparkSession spark;

	public static void main(String[] args) {
		
		boolean testMode = args.length == 0;
		
		if (testMode) {
			spark = SparkSession.builder().appName("ViewingFigures").master("local[*]").getOrCreate();
		} else {
			spark = SparkSession.builder().appName("ViewingFigures").getOrCreate();
		}
		
		String viewPath = "src/main/resources/views-1.csv";
		String chaptersPath = "src/main/resources/chapters.csv";
		String titlesPath = "src/main/resources/titles.csv";
		
		Dataset<Row> viewDF = readViews(viewPath, testMode);
		Dataset<Row> chaptersDF = readChapters(chaptersPath, testMode);
		Dataset<Row> titleDF = readTitles(titlesPath, testMode);

		viewDF.show();
		chaptersDF.show();
		titleDF.show();
		
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
