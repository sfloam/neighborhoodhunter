//Inspiration from https://stackoverflow.com/questions/44516627/how-to-convert-a-dataframe-to-dataset-in-apache-spark-in-scala
//Inspiration from https://stackoverflow.com/questions/37851606/how-to-join-datasets-on-multiple-columns

val dfsl = df_combined_School_Locations.select("Year2","DBN2","Latitude","Longitude")
val dfsemshs = df_combined_EMS_HS.select("*")

case class schoollocs(Year2:  Int, DBN2:  String, Latitude: Double, Longitude:Double)
val ds2:Dataset[schoollocs] = dfsl.as[schoollocs]


case class schooldata(Year: Int, DBN: String, School_Name: String, School_Type: String, Weight:Double)
val ds1:Dataset[schooldata] = dfsemshs.as[schooldata]


val schoolswithgps = ds1.join(ds2).where(ds1("Year") === ds2("Year2")).where(ds1("DBN") === ds2("DBN2")).drop("Year2","DBN2")


val df_school_2015 = schoolswithgps.select("*").where("Year=2015").withColumnRenamed("Year","Year_2015").withColumnRenamed("DBN","DBN_2015").withColumnRenamed("School_Name","School_Name_2015").withColumnRenamed("School_Type","School_Type_2015").withColumnRenamed("Weight","Weight_2015").withColumnRenamed("Latitude","Latitude_2015").withColumnRenamed("Longitude","Longitude_2015")


val df_school_2016 = schoolswithgps.select("*").where("Year=2016").withColumnRenamed("Year","Year_2016").withColumnRenamed("DBN","DBN_2016").withColumnRenamed("School_Name","School_Name_2016").withColumnRenamed("School_Type","School_Type_2016").withColumnRenamed("Weight","Weight_2016").withColumnRenamed("Latitude","Latitude_2016").withColumnRenamed("Longitude","Longitude_2016")

val df_school_2017 = schoolswithgps.select("*").where("Year=2017").withColumnRenamed("Year","Year_2017").withColumnRenamed("DBN","DBN_2017").withColumnRenamed("School_Name","School_Name_2017").withColumnRenamed("School_Type","School_Type_2017").withColumnRenamed("Weight","Weight_2017").withColumnRenamed("Latitude","Latitude_2017").withColumnRenamed("Longitude","Longitude_2017")



val df_school_2015_2016 = df_school_2015.join(df_school_2016).where(df_school_2016("DBN_2016") === df_school_2015("DBN_2015"))
val df_school_2015_2016_2017 = df_school_2015_2016.join(df_school_2017).where(df_school_2017("DBN_2017") === df_school_2015_2016("DBN_2015"))
val df_school_remove_cols = df_school_2015_2016_2017.drop("Year_2015","Year_2016","Year_2017","School_Type_2015","School_Type_2016","School_Type_2017","School_Name_2015","School_Name_2016","School_Name_2017","DBN_2015","DBN_2016","Latitude_2015","Latitude_2016","Longitude_2016","Longitude_2015")
val df_school_rename = df_school_remove_cols.withColumnRenamed("DBN_2017","DBN").withColumnRenamed("Latitude_2017","Latitude").withColumnRenamed("Longitude_2017","Longitude")
val df_school_complete = df_school_rename.select("DBN","Weight_2015","Weight_2016","Weight_2017","Latitude","Longitude").dropDuplicates


val dfcrdd = df_school_complete.rdd

val dfrdddone = dfcrdd.map(line => org.apache.spark.sql.Row(
        line(0),//dbn
        line(1),//Weight_2015
        line(2),//Weight_2016
        line(3),//Weight_2017
        ((line(3).toString.toDouble - ((line(1).toString.toDouble + line(2).toString.toDouble)/2))/ line(3).toString.toDouble),//var
        line(4),
        line(5),
        "["+line(4).toString+","+line(5).toString+"]"))


val dfcrdd1schema = StructType(
        StructField("DBN",StringType,true)::
        StructField("Weight_2015",DoubleType,true)::
        StructField("Weight_2016",DoubleType,true)::
        StructField("Weight_2017",DoubleType,true)::
        StructField("Variance",DoubleType,true)::
        StructField("Latitude",DoubleType,true)::
        StructField("Longitude",DoubleType,true)::
        StructField("Coordinates",StringType,true)::
        Nil)
    


val df_school_no_neighs = sqlContext.createDataFrame(dfrdddone,dfcrdd1schema)


val df_neighborhood_centroids = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/geodata/LookupTable.csv")

val df_neighborhood_centroids_casted = df_neighborhood_centroids.selectExpr("Name",
    "Long_Lat",
    "Borough",
    "Precinct",
    "cast(Longitude as double) Longitude",
    "cast(Latitude as double) Latitude")

val lookups = df_neighborhood_centroids_casted.rdd.collect

def getNeighborhood(latitude:Double,longitude:Double,lookups:Array[org.apache.spark.sql.Row] ):String = {
    var minDist = Double.MaxValue
    var minDistNeighborhood = "Not Found"
    for (arr <- lookups) {
        val lat = arr(5).toString.toDouble - latitude
        val lon = math.abs(arr(4).toString.toDouble) - math.abs(longitude)
        val d =  math.sqrt(math.pow(lat,2) + math.pow(lon,2))

        if (minDist>d){
            minDist = d
            minDistNeighborhood = arr(0).toString
            //println(minDistNeighborhood,d)
        }
    }
    return(minDistNeighborhood)
}

val df_school_neighs = df_school_no_neighs.rdd.map(line=> org.apache.spark.sql.Row(line(0),line(1),line(2),line(3),line(4),line(5),line(6),line(7),getNeighborhood(line(5).toString.toDouble,line(6).toString.toDouble,lookups)))

val dfcrddschema = StructType(
        StructField("DBN",StringType,true)::
        StructField("Weight_2015",DoubleType,true)::
        StructField("Weight_2016",DoubleType,true)::
        StructField("Weight_2017",DoubleType,true)::
        StructField("Variance",DoubleType,true)::
        StructField("Latitude",DoubleType,true)::
        StructField("Longitude",DoubleType,true)::
        StructField("Coordinates",StringType,true)::
        StructField("Neighborhood",StringType,true)::
        Nil)

val df_school = sqlContext.createDataFrame(df_school_neighs,dfcrddschema)

df_school.rdd.zipWithIndex.map(tup => "{\"type\":\"Feature\","+
    "\"id\":"+ tup._2.toString + ","+
    "\"properties\": {"+
    "\"DBN\":"+ "\""+tup._1(0).toString+"\""+ "," +
    "\"Weight_2015\":" + "\""+ tup._1(1).toString + "\"," +
    "\"Weight_2016\":"+ tup._1(2).toString + "," +
    "\"Weight_2017\":"+ tup._1(3).toString + "," +
    "\"Variance\":"+ tup._1(4).toString + "," +
    "\"Latitude\":"+ tup._1(5).toString + "," +
    "\"Longitude\":"+ tup._1(6).toString + "," +
    "\"Neighborhood\":"+ tup._1(8).toString + "}," +
    "\"geometry\": {\"type\":\"Point\", \"coordinates\":"+tup._1(7) +"}},")

