// Program joins data from all 3 data sources : Housing, Crime, and School
// The model works by combining variance scores from each data source and assigning a weight to each.
// The neighbohoods with the highest scores show the greatest change in housing price increase, crime decresae, and school quality increase.
// The applicable neighborhoods with the highest rank are then surfaced in the UI for the user and filtered by user input for budget and building type.

// This program contains 2 outputs
// 1) The input which must be used the geo json builder class in order to generate the UI.
// 2) review of model goodness, which was run for various differnt weights (hard coded below). Best weights listed.

//spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

// Running on Spark 2.2.0 version
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf 
import scala.collection.mutable.ArrayBuffer 
import sqlContext._
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.exp
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{min, max, lit}

val sqlContext = new SQLContext(sc)

var crime = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/CrimeWithPrediction.csv")
var housing = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/historical_all_buildingType_5.1.18")
var school = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/NYC_School_Data.csv")
var recentPrice = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/summary_2017_2018_5.2.2018")


housing = housing.select(
  $"NEIGHBORHOOD",
  $"BOROUGH",
  $"BUILDING_CLASS_CATEGORY",
  $"Variance",
  $"Variance_prediction",
  $"Variance_Test").
  withColumnRenamed("Variance", "Variance_housing").
  withColumnRenamed("Variance_prediction", "Variance_prediction_housing").
  withColumnRenamed("Variance_Test", "Variance_test_housing").
  withColumnRenamed("NEIGHBORHOOD", "Neighborhood")
  

crime = crime.select(
  $"Name",
  $"PCT",
  $"Variance",
  $"Variance_prediction").
  withColumnRenamed("Name", "Neighborhood").
  withColumnRenamed("Variance", "Variance_crime").
  withColumnRenamed("Variance_prediction", "Variance_prediction_crime")


  school = school.select(
    $"Neighborhood",
    $"Variance",
    $"Variance_prediction").
    withColumnRenamed("Variance", "Variance_school").
    withColumnRenamed("Variance_prediction", "Variance_prediction_school")


//currently selecting the average sale price for sfiltering against user budget input
  recentPrice = recentPrice.select(
    $"NEIGHBORHOOD",
    $"BUILDING_CLASS_CATEGORY",
    $"MIN_SALE_PRICE_2017_2018",
    $"MAX_SALE_PRICE_2017_2018",
    $"AVERAGE_SALE_PRICE_2017_2018",
    $"NUMBER_OF_SALES").
    withColumnRenamed("NEIGHBORHOOD", "Neighborhood").
    withColumnRenamed("AVERAGE_SALE_PRICE_2017_2018", "price")

val priceDouble = recentPrice.selectExpr(
    "Neighborhood",
    "BUILDING_CLASS_CATEGORY",
    "cast(MIN_SALE_PRICE_2017_2018 as Double) MIN_SALE_PRICE_2017_2018",
    "cast(MAX_SALE_PRICE_2017_2018 as Double) MAX_SALE_PRICE_2017_2018",
    "cast(price as Double) price",
    "NUMBER_OF_SALES")


var join1 = housing.join(crime, Seq("Neighborhood"))

var join2 = join1.join(school, Seq("Neighborhood"))

val join2Double = join2.selectExpr(
    "Neighborhood", 
    "BOROUGH",
    "BUILDING_CLASS_CATEGORY", 
    "PCT",
    "cast(Variance_housing as Double) Variance_housing",
    "cast(Variance_crime as Double) Variance_crime",
    "cast(Variance_school as Double) Variance_school",
    "cast(Variance_prediction_housing as Double) Variance_prediction_housing",
    "cast(Variance_prediction_crime as Double) Variance_prediction_crime",
    "cast(Variance_prediction_school as Double) Variance_prediction_school",
    "cast(Variance_test_housing as Double) Variance_test_housing")

     
//set weights for each parameter
val housing_weight = .35
val crime_weight = .4
val school_weight = .25


// evaluate the score
val scoreDF = join2Double.withColumn("score", 
  ($"Variance_housing" * housing_weight) + (-($"Variance_crime") * crime_weight) + ($"Variance_school" * school_weight))


//evaluate the rank. rank is normalized so that range is known for all input and so you could create a heat map with it
val (scoreMin, scoreMax) = scoreDF.agg(min($"score"), max($"score")).first match {
  case Row(x: Double, y: Double) => (x, y)
}

val scaledRange = lit(99) 
val scaledMin = lit(0)  
val normalized = ($"score" - scoreMin) / (scoreMax - scoreMin)
val scaled = scaledRange * normalized + scaledMin
val rankDF = scoreDF.withColumn("rank", scaled).sort(desc("rank"))


//evaluate predicated score for 2017
val scorePrediction = rankDF.withColumn("score_prediction", 
  ($"Variance_prediction_housing" * housing_weight) + (-($"Variance_prediction_crime") * crime_weight) + ($"Variance_prediction_school" * school_weight))


//evaluate predicated rank for 2017 - normalized score_prediction, this is used for testing our measure of goodness
// we measure  "rank_predicted" against "rank_testing"
val (scoreMinP, scoreMaxP) = scorePrediction.agg(min($"score_prediction"), max($"score_prediction")).first match {
  case Row(x: Double, y: Double) => (x, y)
}

val scaledRangeP = lit(100) 
val scaledMinP = lit(0)  
val normalizedP = ($"score_prediction" - scoreMinP) / (scoreMaxP - scoreMinP)
val scaledP = scaledRangeP * normalizedP + scaledMinP
val rankPrediction = scorePrediction.withColumn("rank_predicted", scaledP).sort(desc("rank"))



//evaluate test rank for 2017 - normalized Variance_test_housing, this is used for testing our measure of goodness
// we measure  "rank_predicted" against "rank_testing"
// we then look at the output file to see:
// 1) how many neighborhoods in our top 10 rank are in the top 10 rank_testing neighborhood list
// 2) how many neighborhoods in our top 20 rank are in the top 20 rank_testing neighborhood list
// 3) other qualitative comparisons
val (scoreMinT, scoreMaxT) = rankPrediction.agg(min($"Variance_test_housing"), max($"Variance_test_housing")).first match {
  case Row(x: Double, y: Double) => (x, y)
}
val scaledRangeT = lit(100)
val scaledMinT = lit(0)  
val normalizedT = ($"Variance_test_housing" - scoreMinT) / (scoreMaxT - scoreMinT)
val scaledT = scaledRangeT * normalizedT + scaledMinT
val rankTesting = rankPrediction.withColumn("rank_testing", scaledT)


//join with price for filtering data by user inputs. Join maintains all building types
val priceDF = rankTesting.join(priceDouble, Seq("Neighborhood", "BUILDING_CLASS_CATEGORY"), "left_outer")

//if recent data does not have a price, price values and rank set to -1 so that results are not surfaced in top rank
val mapModel = Map("price" -> -1.0, "MAX_SALE_PRICE_2017_2018" -> -1.0, "MIN_SALE_PRICE_2017_2018" -> -1.0, 
    "rank" -> -1.0, "rank_testing" -> -1.0, "rank_predicted" -> -1.0)

val replaceNullModel = priceDF.na.fill(mapModel)

val replaceRankPrice = replaceNullModel.
  withColumn("rank", 
    when(col("price") === -1.0, -1.0).
    otherwise(col("rank"))).
  withColumn("rank_predicted", 
    when(col("price") === -1.0, -1.0).
    otherwise(col("rank_predicted"))).
  withColumn("rank_testing", 
    when(col("price") === -1.0, -1.0).
    otherwise(col("rank_testing"))).
  withColumn("score_prediction", 
    when(col("price") === -1.0, -1.0).
    otherwise(col("score_prediction")))

// format input for the geo json builder
val output = replaceRankPrice.select(
  $"Neighborhood",
  $"BUILDING_CLASS_CATEGORY",
  $"price",
  $"rank",
  $"score").
  withColumnRenamed("Neighborhood", "neighborhood").
  withColumnRenamed("BUILDING_CLASS_CATEGORY", "building_type").
  sort(desc("rank"))


//create strings for generated files
val count = 4
val fileName = "/user/sc2936/housingSalesClean/Goodness_H." + housing_weight +"_C." + crime_weight + "_S." + school_weight + "_" + count
val outputName = "/user/sc2936/housingSalesClean/Output_H." + housing_weight +"_C." + crime_weight + "_S." + school_weight + "_" + count

//file contains all of the goodness measure
replaceRankPrice.coalesce(1).write.option("header", "true").format("csv").save(fileName)

//file contains input needed for the gro json builder
output.coalesce(1).write.option("header", "true").format("csv").save(outputName)

val readFile = "housingSalesClean/Goodness_H." + housing_weight +"_C." + crime_weight + "_S." + school_weight + "_" + count
//val test = sqlContext.read.format("csv").option("header", "true").load(readFile)



