// Running on Spark 2.2.0 version
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType

val sqlContext = new SQLContext(sc)
var crime = sqlContext.read.format("csv").option("header", "true").load("BDAD/Sample/Crime_sample.csv")
var school = sqlContext.read.format("csv").option("header", "true").load("BDAD/Sample/School_sample.csv")
var housing = sqlContext.read.format("csv").option("header", "true").load("BDAD/Sample/Housing_sample.csv")

crime.show
school.show
housing.show

crime.printSchema
school.printSchema
housing.printSchema

crime = crime.toDF("PCT", "Neighborhood", "Borough", "2015", "2016", "2017", "Avg","Diff","Variance")

var join1 = housing.join(crime, Seq("Neighborhood"))
join1 = join1.drop("2015", "2016", "2017", "Avg", "Diff")
join1 = join1.toDF("Neighborhood", "BuildingType", "Variance_housing", "PCT", "Borough", "Variance_crime")
join1.printSchema
join1.show

var join2 = join1.join(school, Seq("Neighborhood"))
join2 = join2.drop("Weight_2015", "Weight_2016", "Weight_2017")
join2 = join2.toDF("Neighborhood", "BuildingType", "Variance_housing", "PCT", "Borough", "Variance_crime", "Variance_school")
join2.printSchema
join2.show

join2 = join2.select(
   join2.columns.map {
     case "Variance_housing" => join2("Variance_housing").cast(DoubleType).as("Variance_housing")
     case "Variance_crime" => join2("Variance_crime").cast(DoubleType).as("Variance_crime")
     case "Variance_school" => join2("Variance_school").cast(DoubleType).as("Variance_school")
     case other => join2(other)
     }: _*
) 

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val housing_weight = args(0).toDouble
val crime_weight = args(1).toDouble
val school_weight = args(2).toDouble

var score = join2.withColumn("Score", ($"Variance_housing" * housing_weight) + (-($"Variance_crime") * crime_weight) + ($"Variance_school" * school_weight))
score = score.sort(asc("Score"))
score.printSchema
score.show

//Running on dumbo home folder
//$ spark-shell -i ~/Model.scala --conf spark.driver.args="0.5 0.3 0.2"