
//=========== 2014-2015 ===========
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

val sqlContext = new SQLContext(sc)

import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType, DoubleType};

cleanedData.map(line=>(line(0),line(9),line(line.size-1))).map(tup=>(tup._1, tup._2, tup._3.replace("[\"{\\\"address\\\":\\\"",""))

val spark = SparkSession.builder().getOrCreate()

//load json file
val df_2014_2015 = spark.read.json(spark.sparkContext.wholeTextFiles("hdfs:///user/smf463/bdad-project/educationData/SCHOOL_LOCATIONS_2014_2015_JSON.json").values)

//convert df to rdd
val dfToRDD_2014_2015 = df_2014_2015.rdd

//clean data
val df_School_Locations_2014_2015_rdd = dfToRDD_2014_2015.map(line => line.get(0).toString).
    map(st=>st.
        replace("WrappedArray(","FLOAM").
        replace("]),","]").
        replace("FLOAMFLOAM","").
        replaceAll("\\s+", " ")).
    map(fin=>fin.split("FLOAM")).
    flatMap(rddline=>rddline).
    map(line=>line.split(", ")).
    map(line=>(line(0), line(9), line(line.size-1))).
    map(tup=>(tup._1, tup._2, tup._3.
        replace("[\"{\\\"address\\\":\\\"","").
        replace("\\\",\\\"city\\\":\\\"",",").
        replace("\\\",\\\"state\\\":\\\"",",").
        replace("\\\",\\\"zip\\\":\\\"",",").
        replace("\\\"}","").replace("] ","").
        replace("\"","").
        split(","))).
    filter(tup=> !tup._3(0).contains("[null")).
    map(tup=>(tup._2.replace(" ",""), tup._3(0), tup._3(1), tup._3(2), tup._3(3), tup._3(4), tup._3(5))).
    map(line => org.apache.spark.sql.Row(2014,line._1,line._2,line._3,line._4,line._5,line._6,line._7))


//set up schema
val df_School_Locations_2014_2015_schema = StructType(
        StructField("Year",IntegerType,true)::
        StructField("DBN",StringType,true)::
        StructField("Street",StringType,true)::
        StructField("City",StringType,true)::
        StructField("State",StringType,true)::
        StructField("Zip",StringType,true)::
        StructField("Latitude",StringType,true)::
        StructField("Longitude",StringType,true)::Nil)

//Create new DF
val df_School_Locations_2014_2015_ready = sqlContext.createDataFrame(df_School_Locations_2014_2015_rdd,df_School_Locations_2014_2015_schema)

//Create new DF with updated types
val df_School_Locations_2014_2015_final = df_School_Locations_2014_2015_ready.selectExpr("Year",
    "DBN",
    "Street",
    "City",
    "State",
    "Zip",
    "cast(Latitude as double) Latitude",
    "cast(Longitude as double) Longitude")


//=========== 2015-2016 ===========

val spark = SparkSession.builder().getOrCreate()

//load json file
val df_2015_2016 = spark.read.json(spark.sparkContext.wholeTextFiles("hdfs:///user/smf463/bdad-project/educationData/SCHOOL_LOCATIONS_2015_2016_JSON.json").values)

//convert df to rdd
val dfToRDD_2015_2016 = df_2015_2016.rdd

//clean data
val df_School_Locations_2015_2016_rdd = dfToRDD_2015_2016.map(line => line.get(0).toString).
    map(st=>st.
        replace("WrappedArray(","FLOAM").
        replace("]),","]").
        replace("FLOAMFLOAM","").
        replaceAll("\\s+", " ")).
    map(fin=>fin.split("FLOAM")).
    flatMap(rddline=>rddline).
    map(line=>line.split(", ")).
    map(line=>(line(0),line(9),line(line.size-1))).
    map(tup=>(tup._1, tup._2, tup._3.
        replace("[\"{\\\"address\\\":\\\"","").
        replace("\\\",\\\"city\\\":\\\"",",").
        replace("\\\",\\\"state\\\":\\\"",",").
        replace("\\\",\\\"zip\\\":\\\"",",").
        replace("\\\"}","").
        replace("] ","").
        replace("\"","").
        split(","))).
    filter(tup=> !tup._3(0).contains("[null")).
    map(tup=>(tup._2.replace(" ",""), tup._3(0), tup._3(1), tup._3(2), tup._3(3), tup._3(4), tup._3(5))).
    map(line => org.apache.spark.sql.Row(2015,line._1,line._2,line._3,line._4,line._5,line._6,line._7))


//set up schema
val df_School_Locations_2015_2016_schema = StructType(
        StructField("Year",IntegerType,true)::
        StructField("DBN",StringType,true)::
        StructField("Street",StringType,true)::
        StructField("City",StringType,true)::
        StructField("State",StringType,true)::
        StructField("Zip",StringType,true)::
        StructField("Latitude",StringType,true)::
        StructField("Longitude",StringType,true)::Nil)

//Create new DF
val df_School_Locations_2015_2016_ready = sqlContext.createDataFrame(df_School_Locations_2015_2016_rdd,df_School_Locations_2015_2016_schema)

//Create new DF with updated types
val df_School_Locations_2015_2016_final = df_School_Locations_2015_2016_ready.selectExpr("Year",
    "DBN",
    "Street",
    "City",
    "State",
    "Zip",
    "cast(Latitude as double) Latitude",
    "cast(Longitude as double) Longitude")


//=========== 2016-2017 ===========

val spark = SparkSession.builder().getOrCreate()

//load json file
val df_2016_2017 = spark.read.json(spark.sparkContext.wholeTextFiles("hdfs:///user/smf463/bdad-project/educationData/SCHOOL_LOCATIONS_2016_2017_JSON.json").values)

//convert df to rdd
val dfToRDD_2016_2017 = df_2016_2017.rdd

//clean data 
val df_School_Locations_2016_2017_rdd = dfToRDD_2016_2017.map(line => line.get(0).toString).
    map(st=>st.
        replace("WrappedArray(","FLOAM").
        replace("]),","]").replace("FLOAMFLOAM","").
        replaceAll("\\s+", " ")).
    map(fin=>fin.split("FLOAM")).
    flatMap(rddline=>rddline).
    map(line=>line.split(", ")).
    map(line=>(line(0),line(9),line(line.size-1))).
    map(tup=>(tup._1, tup._2, tup._3.
        replace("[\"{\\\"address\\\":\\\"","").
        replace("\\\",\\\"city\\\":\\\"",",").
        replace("\\\",\\\"state\\\":\\\"",",").
        replace("\\\",\\\"zip\\\":\\\"",",").
        replace("\\\"}","").
        replace("] ","").
        replace("\"","").
        split(","))).
    filter(tup=> !tup._3(0).contains("[null")).
    map(tup=>(tup._2.replace(" ",""), tup._3(0), tup._3(1), tup._3(2), tup._3(3), tup._3(4), tup._3(5))).
    map(line => org.apache.spark.sql.Row(2016,line._1,line._2,line._3,line._4,line._5,line._6,line._7))


//set up schema
val df_School_Locations_2016_2017_schema = StructType(
        StructField("Year",IntegerType,true)::
        StructField("DBN",StringType,true)::
        StructField("Street",StringType,true)::
        StructField("City",StringType,true)::
        StructField("State",StringType,true)::
        StructField("Zip",StringType,true)::
        StructField("Latitude",StringType,true)::
        StructField("Longitude",StringType,true)::Nil)

//Create new DF
val df_School_Locations_2016_2017_ready = sqlContext.createDataFrame(df_School_Locations_2016_2017_rdd,df_School_Locations_2016_2017_schema)

//Create new DF with updated types
val df_School_Locations_2016_2017_final = df_School_Locations_2016_2017_ready.selectExpr("Year",
    "DBN",
    "Street",
    "City",
    "State",
    "Zip",
    "cast(Latitude as double) Latitude",
    "cast(Longitude as double) Longitude")


//=========== ALL SCHOOL LOCATIONS FOR 2014-2017 ===========

//combine them
val df_combined_School_Locations = df_School_Locations_2016_2017_final.union(df_School_Locations_2015_2016_final).union(df_School_Locations_2014_2015_final)
