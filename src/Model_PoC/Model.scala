//Running on dumbo home folder
//$ spark-shell -i ~/Model.scala --conf spark.driver.args="0.5 0.3 0.2"
//$ spark-shell -i ~/Model.scala --conf spark.driver.args="0.45 0.3 0.25"
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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.functions.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{min, max, lit}

val sqlContext = new SQLContext(sc)

var crime = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/CrimeWithPrediction.csv")
var housing = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/historical_all_buildingType_4.28.18")
var school = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/NYC_School_Data.csv")


var recentPrice = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/summary_2017_2018_4.28.2018")


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


//currently selecting the average sale price for scrubbing
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

     

// get the command line args and cast it to double 
//val args = sc.getConf.get("spark.driver.args").split("\\s+")
// val housing_weight = args(0).toDouble
// val crime_weight = args(1).toDouble
// val school_weight = args(2).toDouble

val housing_weight = .35
val crime_weight = .4
val school_weight = .25


// evaluate the score
var scoreDF = join2Double.withColumn("score", 
  ($"Variance_housing" * housing_weight) + (-($"Variance_crime") * crime_weight) + ($"Variance_school" * school_weight))


//evaluate the rank. rank is normalized so that you could create a heat map with it
val (scoreMin, scoreMax) = scoreDF.agg(min($"score"), max($"score")).first match {
  case Row(x: Double, y: Double) => (x, y)
}

val scaledRange = lit(100) 
val scaledMin = lit(0)  
val normalized = ($"score" - scoreMin) / (scoreMax - scoreMin)
val scaled = scaledRange * normalized + scaledMin
val rankDF = scoreDF.withColumn("rank", scaled).sort(desc("rank"))


//evaluate predicated score for 2017
val scorePrediction = rankDF.withColumn("score_prediction", 
  ($"Variance_prediction_housing" * housing_weight) + (-($"Variance_prediction_crime") * crime_weight) + ($"Variance_prediction_school" * school_weight))



//evaluate predicated rank for 2017
val (scoreMinP, scoreMaxP) = scorePrediction.agg(min($"score_prediction"), max($"score_prediction")).first match {
  case Row(x: Double, y: Double) => (x, y)
}

val scaledRangeP = lit(100) 
val scaledMinP = lit(0)  
val normalizedP = ($"score_prediction" - scoreMinP) / (scoreMaxP - scoreMinP)
val scaledP = scaledRangeP * normalizedP + scaledMinP
val rankPrediction = scorePrediction.withColumn("rank_predicted", scaledP).sort(desc("rank"))



//evaluate test rank for 2017
val (scoreMinT, scoreMaxT) = rankPrediction.agg(min($"Variance_test_housing"), max($"Variance_test_housing")).first match {
  case Row(x: Double, y: Double) => (x, y)
}
val scaledRangeT = lit(100)
val scaledMinT = lit(0)  
val normalizedT = ($"Variance_test_housing" - scoreMinT) / (scoreMaxT - scoreMinT)
val scaledT = scaledRangeT * normalizedT + scaledMinT
val rankTesting = rankPrediction.withColumn("rank_testing", scaledT)



//evaluate goodness
val rankPredTest = rankTesting.withColumn("rank_pred_eval", $"rank_testing" - $"rank_predicted")

//add results to some df for each loop with inputs and sum of rankPredTest


//alternative way of calculating rank with MinMaxScaler
//using MinMaxScaler - resulting rank is in a vector and can't exrtact from DF. Must extract for sorting
  // val vectorizeCol = udf( (score:Double) => Vectors.dense(Array(score)) )
  // val rankDF = scoreDF.withColumn("scoreVec", vectorizeCol(scoreDF("score")))

  // val scaler = new MinMaxScaler().
  //   setInputCol("scoreVec").
  //   setOutputCol("rankVec").
  //   setMax(100).
  //   setMin(0)

  // val scaled = scaler.fit(rankDF).transform(rankDF)


//join with price for filtering data by user inputs. Join maintains all building types
val priceDF = rankPredTest.join(priceDouble, Seq("Neighborhood", "BUILDING_CLASS_CATEGORY"), "left_outer")

//if recent data does not have a price, all values set to -1
val mapModel = Map("price" -> -1.0, "MAX_SALE_PRICE_2017_2018" -> -1.0, "MIN_SALE_PRICE_2017_2018" -> -1.0, 
    "rank" -> -1.0, "rank_testing" -> -1.0, "rank_predicted" -> -1.0)

val replaceNullModel = priceDF.na.fill(mapModel)


//set the rank to -1 when there is no price avaialable from the last 12 months
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

val output = replaceRankPrice.select(
  $"Neighborhood",
  $"BUILDING_CLASS_CATEGORY",
  $"price",
  $"rank",
  $"score").
  withColumnRenamed("Neighborhood", "neighborhood").
  withColumnRenamed("BUILDING_CLASS_CATEGORY", "building_type").
  sort(desc("rank"))


  val count = 1
  val fileName = "/user/sc2936/housingSalesClean/Goodness_H." + housing_weight +"_C." + crime_weight + "_S." + school_weight + "_" + count
  val outputName = "/user/sc2936/housingSalesClean/Output_H." + housing_weight +"_C." + crime_weight + "_S." + school_weight


  replaceRankPrice.coalesce(1).write.option("header", "true").format("csv").save(fileName)

  //output only needed for final weights
  //output.coalesce(1).write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/Weights1_H.45_C.3_S.25")

  val readFile = "housingSalesClean/Goodness_H." + housing_weight +"_C." + crime_weight + "_S." + school_weight + "_" + count
  val test = sqlContext.read.format("csv").option("header", "true").load(readFile)


  




