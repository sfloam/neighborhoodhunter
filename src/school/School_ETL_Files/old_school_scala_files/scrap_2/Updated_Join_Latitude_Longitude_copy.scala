//Inspiration from https://stackoverflow.com/questions/44516627/how-to-convert-a-dataframe-to-dataset-in-apache-spark-in-scala
//Inspiration from https://stackoverflow.com/questions/37851606/how-to-join-datasets-on-multiple-columns

val dfsl = df_combined_School_Locations.select("Year2","DBN2","Latitude","Longitude")
val dfsemshs = df_combined_EMS_HS.select("*")

case class schoollocs(Year2:  Int, DBN2:  String, Latitude: Double, Longitude:Double)
val ds2:Dataset[schoollocs] = dfsl.as[schoollocs]


case class schooldata(Year: Int, DBN: String, School_Name: String, School_Type: String, Weight:Double)
val ds1:Dataset[schooldata] = dfsemshs.as[schooldata]



//val schoolswithgps = ds1.as('ds1).join(ds2.as('ds2),$"ds1.Year" === $"ds2.Year2" && $"ds1.DBN" === $"ds2.DBN2")


val df_school_2015 = schoolswithgps.select("*").where("Year=2015").withColumnRenamed("Year","Year_2015").withColumnRenamed("DBN","DBN_2015").withColumnRenamed("School_Name","School_Name_2015").withColumnRenamed("School_Type","School_Type_2015").withColumnRenamed("Weight","Weight_2015").withColumnRenamed("Latitude","Latitude_2015").withColumnRenamed("Longitude","Longitude_2015")

val df_school_2016 = schoolswithgps.select("*").where("Year=2016").withColumnRenamed("Year","Year_2016").withColumnRenamed("DBN","DBN_2016").withColumnRenamed("School_Name","School_Name_2016").withColumnRenamed("School_Type","School_Type_2016").withColumnRenamed("Weight","Weight_2016").withColumnRenamed("Latitude","Latitude_2016").withColumnRenamed("Longitude","Longitude_2016")

val df_school_2017 = schoolswithgps.select("*").where("Year=2017").withColumnRenamed("Year","Year_2017").withColumnRenamed("DBN","DBN_2017").withColumnRenamed("School_Name","School_Name_2017").withColumnRenamed("School_Type","School_Type_2017").withColumnRenamed("Weight","Weight_2017").withColumnRenamed("Latitude","Latitude_2017").withColumnRenamed("Longitude","Longitude_2017")








val df_school_2015_2016 = df_school_2015.join(df_school_2016).where(df_school_2016("DBN_2016") === df_school_2015("DBN_2015"))

val df_school_2015_2016_2017 = df_school_2015_2016.join(df_school_2017).where(df_school_2017("DBN_2017") === df_school_2015_2016("DBN_2015"))

val df_school_remove_cols = df_school_2015_2016_2017.drop("Year_2015","Year_2016","Year_2017","School_Type_2015","School_Type_2016","School_Type_2017","School_Name_2015","School_Name_2016","School_Name_2017","DBN_2015","DBN_2016","Latitude_2015","Latitude_2016","Longitude_2016","Longitude_2015")

val df_school_rename = df_school_remove_cols.withColumnRenamed("DBN_2017","DBN").withColumnRenamed("Latitude_2017","Latitude").withColumnRenamed("Longitude_2017","Longitude")

val df_school_complete = df_school_rename.select("DBN","Weight_2015","Weight_2016","Weight_2017","Latitude","Longitude")
