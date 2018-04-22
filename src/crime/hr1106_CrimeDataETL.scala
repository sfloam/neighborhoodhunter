// Running on Spark 2.2.0 version
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType

val sqlContext = new SQLContext(sc)
var df = sqlContext.read.format("csv").option("header", "true").load("BDAD/project/*")
df = df.withColumn("Weight", input_file_name()) //create a column for filename
df.show
val w = Window.rowsBetween(Window.unboundedPreceding, -1)
df = df.withColumn("PCT", coalesce($"PCT", last($"PCT", true).over(w))) //forward fill precinct number with last non-null value
df.show
df = df.na.fill("-1") //fill null or empty values with -1
df = df.filter($"PCT"=!="DOC") //remove DOC crime data
df.show
//replace filename with the corresponding weight (used for internal algorithm) Which is defined based on the crime's severiety
df = df.withColumn("Weight", when(col("Weight").equalTo("hdfs://dumbo/user/hr1106/BDAD/project/misdemeanor-offenses-by-precinct-2000-2017.csv"), "0.8").otherwise(col("Weight")))
df = df.withColumn("Weight", when(col("Weight").equalTo("hdfs://dumbo/user/hr1106/BDAD/project/non-seven-major-felony-offenses-by-precinct-2000-2017.csv"), "0.9").otherwise(col("Weight")))
df = df.withColumn("Weight", when(col("Weight").equalTo("hdfs://dumbo/user/hr1106/BDAD/project/seven-major-felony-offenses-by-precinct-2000-2017.csv"), "1").otherwise(col("Weight")))
df = df.withColumn("Weight", when(col("Weight").equalTo("hdfs://dumbo/user/hr1106/BDAD/project/violation-offenses-by-precinct-2000-2017.csv"), "0.7").otherwise(col("Weight")))
df.show

//defining schema
df = df.select(
   df.columns.map {
     case "CRIME" => df("CRIME")
     case "PCT" => df(other).cast(IntegerType).as(other)
     case other => df(other).cast(DoubleType).as(other)
   }: _*
)
df.show

//removing the unnecessary columns
df = df.drop("2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012", "2013", "2014")
df.show

df.printSchema

//Remove rows that includes individual crimes
var df2 = df.where($"CRIME".contains("TOTAL"))
df2.show

//Calculate the score of each crime by multiplying it with its weight
df2 = df2.withColumn("2015", $"2015" * $"Weight")
df2 = df2.withColumn("2016", $"2016" * $"Weight")
df2 = df2.withColumn("2017", $"2017" * $"Weight")
df2.show

var df3 = df2.groupBy("PCT").agg(sum("2015").alias("2015"), sum("2016").alias("2016"),sum("2017").alias("2017"))
df3.show

// Evaluating the variance of crime rate from last 4 years to 2017.
// Since 2017 is the most recent year, giving more weightage to it over other years
var df4 = df3.withColumn("Avg", ($"2015" + $"2016") / 2)
df4 = df4.withColumn("Diff", $"2017" - $"Avg")
df4 = df4.withColumn("Variance", $"Diff" / $"2017")
df4 = df4.sort(asc("Variance"))
df4.show

// Load lookup table to map precinct with neighbourhoods
var df5 = sqlContext.read.format("csv").option("header", "true").load("BDAD/NHoodNameCentroids.csv")
//Remove unnecessary columns
df5 = df5.drop("OBJECTID", "the_geom", "Borough", "AnnoLine1", "Stacked", "AnnoLine2", "AnnoLine3", "AnnoAngle")
df5 = df5.select(
   df5.columns.map {
     case "Precinct" => df5("Precinct").cast(IntegerType).as("PCT")
     case other => df5(other)
     }: _*
) 

//Map precinct to neighbourhoods
var df6 = df5.join(df4, Seq("PCT"))

//Sort the neighbourhoods based on crime rate
df6 = df6.sort(asc("Variance"))
df6.show

//Since the analysed and transformed data is small, using coalesce and writing to HDFS
df6.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("BDAD/Crime_SortedNeighbourhood")
