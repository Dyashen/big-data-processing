package be.hogent.dit.tin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;

public class HousePrices {

	public static void main(String[] args) {

		// 1. So... What can we expect?
		SparkSession spark = SparkSession.builder().appName("HousePrices").master("local[*]").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		// Now let's import and put the train and test datasets in pandas dataframe
		Dataset<Row> train = spark.read().format("com.databricks.spark.csv").option("header", true)
				.csv("src/main/resources/train.csv");
		Dataset<Row> test = spark.read().format("com.databricks.spark.csv").option("header", true)
				.csv("src/main/resources/test.csv");

		// display the first five rows of the train and test dataset.
		train.show();
		test.show();

		// Save the 'Id' column
		Dataset<Row> train_ID = train.select("Id");
		train_ID.show();
		Dataset<Row> test_ID = test.select("Id");
		test_ID.show();

		// Now drop the 'Id' colum since it's unnecessary for the prediction process.
		train = train.drop("Id");
		test = test.drop("Id");

		// Deleting outliers
		train = train.where(col("GrLivArea").$less(4000));
		train = train.where(col("SalePrice").$greater(300000));

		// We use the numpy fuction log1p which applies log(1+x) to all elements of the
		// column
		// train = Math.log1p(train.select("SalePrice"));

		// let's first concatenate the train and test data in the same dataframe
		long ntrain = train.count();
		long ntest = test.count();
		Dataset<Row> y_train = train.select("SalePrice");
		y_train.show();

		train.show();
		test.show();

		// Imputing missing values
		Dataset<Row> all_data = train.na().fill("None");
		all_data = all_data.drop("Utilities");

		// transforming some numerical variables that are really categorical
		all_data = all_data.withColumn("MSSubClass", all_data.col("MSSubClass").cast("String"))
				.withColumn("OverallCond", all_data.col("OverallCond").cast("String"))
				.withColumn("YrSold", all_data.col("YrSold").cast("String"))
				.withColumn("MoSold", all_data.col("MoSold").cast("String"))
				.withColumn("SalePrice", all_data.col("SalePrice").cast("long"));

		// Label Encoding some categorical variables that may contain information in
		// their ordering set
		List<String> cols = Arrays.asList("FireplaceQu", "BsmtQual", "BsmtCond", "GarageQual", "GarageCond",
				"ExterQual", "ExterCond", "HeatingQC", "PoolQC", "KitchenQual", "BsmtFinType1", "BsmtFinType2",
				"Functional", "Fence", "BsmtExposure", "GarageFinish", "LandSlope", "LotShape", "PavedDrive", "Street",
				"Alley", "CentralAir", "MSSubClass", "OverallCond", "YrSold", "MoSold");

		List<String> newCols = new ArrayList<String>();

		// process columns, apply LabelEncoder to categorical features
		for (String woord : cols) {
			String output = woord + "_id";
			StringIndexer indexer = new StringIndexer().setHandleInvalid("keep").setInputCol(woord)
					.setOutputCol(output);
			StringIndexerModel model = indexer.fit(all_data);
			all_data = model.transform(all_data);
			newCols.add(output);
			all_data = all_data.drop(woord);
		}

		String[] newColumns = newCols.toArray(new String[0]);
		System.out.println(newColumns.length);

		// Adding one more important feature

		all_data = all_data.withColumn("TotalSF", col("TotalBsmtSF").$plus(col("1stFlrSF")).$plus(col("2ndFlrSF")));

		// Skewed features
		// Dataset<Row> skewness = (Dataset<Row>) all_data.numericColumns();

		// Define a cross validation strategy
		all_data.show();

		/*
		 * Alle features in één vector plaatsen.
		 */
		VectorAssembler assembler = new VectorAssembler().setInputCols(newColumns).setOutputCol("features");

		Dataset<Row> finalData = assembler.transform(all_data);
		finalData.show();

		/*
		 * Eerste manier van scalen.
		 */
		MinMaxScaler minMax = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures");
		MinMaxScalerModel minMaxModel = minMax.fit(finalData);
		Dataset<Row> scaledData = minMaxModel.transform(finalData);

		scaledData = scaledData.select("SalePrice", "features", "scaledFeatures");
		scaledData.show();
		
		
		Dataset<Row>[] splitDataFrames = scaledData.randomSplit(new double[] {0.8, 0.2});
		
		Dataset<Row> trainingSet = splitDataFrames[0];
		Dataset<Row> testSet = splitDataFrames[1];
		

		LinearRegression lr = new LinearRegression()
				.setMaxIter(10)
				.setLabelCol("SalePrice")
				.setRegParam(0.3)
				.setElasticNetParam(0.8);

		// Fit the model.
		LinearRegressionModel lrModel = lr.fit(trainingSet);

		// Print the coefficients and intercept for linear regression.
		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

		// Summarize the model over the training set and print out some metrics.
		LinearRegressionSummary trainingSummary = lrModel.summary();
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());
		System.out.println("MAE: " + trainingSummary.meanAbsoluteError());
		
		Dataset<Row> predictions = lrModel.transform(testSet);
		
		predictions.show();
		
		spark.close();
	}

}
