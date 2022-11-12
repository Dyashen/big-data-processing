package be.hogent.dit.tin;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class ViewingFiguresReport {
	
	SparkSession spark = SparkSession.builder()
			.appName("ViewingFigures")
			.master("local[*]")
			.getOrCreate();

	
	public Dataset<Row> readChapters(String pathname_data, String pathname_header){		
		
		// Data uit de tekstbestanden inlezen.
		Dataset<Row> chapters_data = this.spark.read()
				.csv(pathname_data);
		
		Dataset<Row> chapters_header = spark.read()
				.option("header", true)
				.csv(pathname_header);
		
		Dataset<Row> chapters = chapters_header.union(chapters_data);
		
		return chapters;
	}
	
	public Dataset<Row> readViews(String pathname_data, String pathname_header){
			
			// Data uit de tekstbestanden inlezen.
			Dataset<Row> chapters_data = this.spark.read()
					.csv(pathname_data);
			
			Dataset<Row> views_header = spark.read()
					.option("header", true)
					.csv(pathname_header);
			
			Dataset<Row> views = views_header.union(chapters_data);
			
			return views;
	}
	
	public Dataset<Row> readTitles(String pathname_data, String pathname_header){
		
		// Data uit de tekstbestanden inlezen.
		Dataset<Row> titles_data = this.spark.read()
				.csv(pathname_data);
		
		Dataset<Row> titles_header = spark.read()
				.option("header", true)
				.csv(pathname_header);
		
		Dataset<Row> titles = titles_header.union(titles_data);
		
		return titles;
	}
	
	
	public Dataset<Row> count_chapters_per_course(Dataset<Row> chapters, Dataset<Row> titles){
		
		Column join_expr = titles.col("courseId").equalTo(chapters.col("courseId"));
		return titles.join(chapters, join_expr)
				.select(titles.col("courseId"), titles.col("title"))
				.groupBy(titles.col("courseId"), titles.col("title"))
				.count()
				.orderBy(titles.col("courseId"));
	}
	
	public Dataset<Row> count_viewed_chapters(Dataset<Row> chapters, Dataset<Row> views, Dataset<Row> titles){
		Column joinexpr1 = views.col("chapterId").equalTo(chapters.col("chapterId"));
		Column joinexpr2 = chapters.col("courseId").equalTo(titles.col("courseId"));
		return views.join(chapters, joinexpr1)
				.join(titles, joinexpr2)
				.groupBy(col("userId"), titles.col("courseId"))
				.count();
	}
	
	
	
	public Dataset<Row> calculatepercentage(Dataset<Row> totalchapters, Dataset<Row> totalviews){
		Column joinexpr = totalchapters.col("courseId").equalTo(totalviews.col("courseId"));
		
		return totalchapters.join(totalviews, joinexpr)
				.select(totalchapters.col("courseId"), totalviews.col("count"), totalchapters.col("count"), col("title"))
				.withColumn("percentage", totalviews.col("count").divide(totalchapters.col("count")));
	}
		
	
	
	public static void main(String[] args) {
		
		ViewingFiguresReport vfr_controller = new ViewingFiguresReport();
		Dataset<Row> chapters = vfr_controller.readChapters("src/main/resources/chapters.csv", "src/main/resources/chapters-header.csv");
		Dataset<Row> views = vfr_controller.readViews("src/main/resources/views-1.csv", "src/main/resources/viewsHeader.csv");
		Dataset<Row> titles = vfr_controller.readTitles("src/main/resources/titles.csv", "src/main/resources/titles-header.csv");
		
		
		chapters.show();
		views.show();
		titles.show();
		
		Dataset<Row> totalchapters = vfr_controller.count_chapters_per_course(chapters, titles);
		Dataset<Row> totalviews = vfr_controller.count_viewed_chapters(chapters, views, titles);
		
		Dataset<Row> totalWithPercent = vfr_controller.calculatepercentage(totalchapters, totalviews);
		
		UDF1<Double, Integer> score = new UDF1<Double, Integer>() {

			@Override
			public Integer call(Double percent) throws Exception {
				
				if(percent > 0.9) {
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
		
		vfr_controller.spark.udf().register("berekenScore", score, DataTypes.IntegerType);
		
		Column joinexpr = totalWithPercent.col("courseId").equalTo(titles.col("courseId"));
		
		totalWithPercent
		.select(totalWithPercent.col("courseId"), col("percentage"), col("title"))
		.withColumn("score", functions.callUDF("berekenScore", col("percentage")))
		.drop(col("percentage"))
		.orderBy(desc("score"))
		.show();

	}
}
