


//Taken from McIntosh Notes
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


def getSpecRating(target:Any):java.lang.Integer = {
    if (target == null){null}
    else if (target.toString == "Meeting Target"){3} 
    else if(target.toString == "Approaching Target"){2} 
    else if(target.toString == "Exceeding Target"){4} 
    else if(target.toString == "Not Meeting Target"){1} 
    else{null}
}

def getQRRating(target:Any):java.lang.Integer = {
    if (target == null){null}
    else if(target.toString == "Developing"){2} 
    else if(target.toString == "Proficient"){3} 
    else if(target.toString == "Well Developed"){4} 
    else if(target.toString == "Under Developed"){1} 
    else{null}
}

def getPrincipalScore(target:Any):java.lang.Integer = {
    if (target == null){null}
    else if(target.toString().toDouble < 5 ){1} 
    else if(target.toString().toDouble >= 5 && target.toString().toDouble <10){2} 
    else if(target.toString().toDouble >= 10 && target.toString().toDouble <15){3} 
    else if(target.toString().toDouble >= 15){4}
    else{null}
}

def convertPercentToScore(target:Any):java.lang.Integer = {
    if (target==null){
        null
    }
    else{
        math.ceil((target.toString().toDouble * 100)/25).toInt
    }  
}

def calculateWeight(scores:List[Any]):Double = {
    var cleanScores:List[Int] = scores.filter(s => s!=null).map(s=>s.toString.toInt)
    ((cleanScores.sum+0.0)/cleanScores.size)
}




//====== EMS_2014_2015 ======

//creates data frame from csv with headers included
val df_EMS_SQR_2014_2015_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/EMS_SQR_2014_2015_Summary.csv")

//remove unnecesary columns
val df_EMS_SQR_2014_2015_clean = df_EMS_SQR_2014_2015_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47","Percent Overage/Undercredited","Enrollment","Percent_Students_With_Disabilities","Percent_English_Language_Learners","Percent_Self_Contained")

//convert proficiency levels to numeric values
val EMS_SQR_2014_2015_rdd = df_EMS_SQR_2014_2015_clean.rdd.map(line => org.apache.spark.sql.Row(
        2015, //year
        line(0),//dbn
        line(1),//School name
        line(2),//school type
        calculateWeight(List(getSpecRating(line(3)),getSpecRating(line(4)),getSpecRating(line(5)),getSpecRating(line(6)),getSpecRating(line(7)),getSpecRating(line(8)),getSpecRating(line(9)),getQRRating(line(10)),getQRRating(line(11)),convertPercentToScore(line(12)),convertPercentToScore(line(13)),convertPercentToScore(line(14)),convertPercentToScore(line(15)),convertPercentToScore(line(16)),convertPercentToScore(line(17)),getPrincipalScore(line(18)),convertPercentToScore(line(19)),convertPercentToScore(line(20)),convertPercentToScore(line(21)),convertPercentToScore(line(21))))))

//set up schema
val df_EMS_SQR_2014_2015_schema = StructType(
        StructField("Year",IntegerType,false)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Weight",DoubleType,true)::Nil)
    

//Create new DF
val df_EMS_SQR_2014_2015_ready = sqlContext.createDataFrame(EMS_SQR_2014_2015_rdd,df_EMS_SQR_2014_2015_schema)

//Create new DF with updated types
val df_EMS_SQR_2014_2015_final = df_EMS_SQR_2014_2015_ready.selectExpr("Year","DBN",
    "School_Name",
    "School_Type",
    "Weight")





//====== EMS_2015_2016 ======

//creates data frame from csv with headers included
val df_EMS_SQR_2015_2016_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/EMS_SQR_2015_2016_Summary.csv")

//remove unnecesary columns
val df_EMS_SQR_2015_2016_clean = df_EMS_SQR_2015_2016_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47","Percent Overage/Undercredited","Enrollment","Percent_Students_With_Disabilities","Percent_English_Language_Learners","Percent_Self_Contained")

//convert proficiency levels to numeric values
val EMS_SQR_2015_2016_rdd = df_EMS_SQR_2015_2016_clean.rdd.map(line => org.apache.spark.sql.Row(
        2016, //year
        line(0),//dbn
        line(1),//School name
        line(2),//school type
        calculateWeight(List(getSpecRating(line(3)),getSpecRating(line(4)),getSpecRating(line(5)),getSpecRating(line(6)),getSpecRating(line(7)),getSpecRating(line(8)),getSpecRating(line(9)),getQRRating(line(10)),getQRRating(line(11)),convertPercentToScore(line(12)),convertPercentToScore(line(13)),convertPercentToScore(line(14)),convertPercentToScore(line(15)),convertPercentToScore(line(16)),convertPercentToScore(line(17)),getPrincipalScore(line(18)),convertPercentToScore(line(19)),convertPercentToScore(line(20)),convertPercentToScore(line(21)),convertPercentToScore(line(21))))))

//set up schema
val df_EMS_SQR_2015_2016_schema = StructType(
        StructField("Year",IntegerType,false)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Weight",DoubleType,true)::Nil)
    

//Create new DF
val df_EMS_SQR_2015_2016_ready = sqlContext.createDataFrame(EMS_SQR_2015_2016_rdd,df_EMS_SQR_2015_2016_schema)

//Create new DF with updated types
val df_EMS_SQR_2015_2016_final = df_EMS_SQR_2015_2016_ready.selectExpr("Year","DBN",
    "School_Name",
    "School_Type",
    "Weight")





//====== EMS_2016_2017 ======

//creates data frame from csv with headers included
val df_EMS_SQR_2016_2017_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/EMS_SQR_2016_2017_Summary.csv")

//remove unnecesary columns
val df_EMS_SQR_2016_2017_clean = df_EMS_SQR_2016_2017_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47","Percent Overage/Undercredited","Enrollment","Percent_Students_With_Disabilities","Percent_English_Language_Learners","Percent_Self_Contained")

//convert proficiency levels to numeric values
val EMS_SQR_2016_2017_rdd = df_EMS_SQR_2016_2017_clean.rdd.map(line => org.apache.spark.sql.Row(
        2017, //year
        line(0),//dbn
        line(1),//School name
        line(2),//school type
        calculateWeight(List(getSpecRating(line(3)),getSpecRating(line(4)),getSpecRating(line(5)),getSpecRating(line(6)),getSpecRating(line(7)),getSpecRating(line(8)),getSpecRating(line(9)),getQRRating(line(10)),getQRRating(line(11)),convertPercentToScore(line(12)),convertPercentToScore(line(13)),convertPercentToScore(line(14)),convertPercentToScore(line(15)),convertPercentToScore(line(16)),convertPercentToScore(line(17)),getPrincipalScore(line(18)),convertPercentToScore(line(19)),convertPercentToScore(line(20)),convertPercentToScore(line(21)),convertPercentToScore(line(21))))))

//set up schema
val df_EMS_SQR_2016_2017_schema = StructType(
        StructField("Year",IntegerType,false)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Weight",DoubleType,true)::Nil)
    

//Create new DF
val df_EMS_SQR_2016_2017_ready = sqlContext.createDataFrame(EMS_SQR_2016_2017_rdd,df_EMS_SQR_2016_2017_schema)

//Create new DF with updated types
val df_EMS_SQR_2016_2017_final = df_EMS_SQR_2016_2017_ready.selectExpr("Year","DBN",
    "School_Name",
    "School_Type",
    "Weight")




//====== HS_2014_2015 ======

//creates data frame from csv with headers included
val df_HS_SQR_2014_2015_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/HS_SQR_2014_2015_Summary.csv")

//remove unnecesary columns
val df_HS_SQR_2014_2015_clean = df_HS_SQR_2014_2015_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47","Percent Overage/Undercredited","Enrollment","Percent_Students_With_Disabilities","Percent_English_Language_Learners","Percent_Self_Contained")

//convert proficiency levels to numeric values
val HS_SQR_2014_2015_rdd = df_HS_SQR_2014_2015_clean.rdd.map(line => org.apache.spark.sql.Row(
        2015, //year
        line(0),//dbn
        line(1),//School name
        line(2),//school type
        calculateWeight(List(getSpecRating(line(3)),getSpecRating(line(4)),getSpecRating(line(5)),getSpecRating(line(6)),getSpecRating(line(7)),getSpecRating(line(8)),getSpecRating(line(9)),getQRRating(line(10)),getQRRating(line(11)),convertPercentToScore(line(12)),convertPercentToScore(line(13)),convertPercentToScore(line(14)),convertPercentToScore(line(15)),convertPercentToScore(line(16)),convertPercentToScore(line(17)),getPrincipalScore(line(18)),convertPercentToScore(line(19)),convertPercentToScore(line(20)),convertPercentToScore(line(21)),convertPercentToScore(line(21))))))

//set up schema
val df_HS_SQR_2014_2015_schema = StructType(
        StructField("Year",IntegerType,false)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Weight",DoubleType,true)::Nil)
    

//Create new DF
val df_HS_SQR_2014_2015_ready = sqlContext.createDataFrame(HS_SQR_2014_2015_rdd,df_HS_SQR_2014_2015_schema)

//Create new DF with updated types
val df_HS_SQR_2014_2015_final = df_HS_SQR_2014_2015_ready.selectExpr("Year","DBN",
    "School_Name",
    "School_Type",
    "Weight")


//====== HS_2015_2016 ======


//creates data frame from csv with headers included
val df_HS_SQR_2015_2016_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/HS_SQR_2015_2016_Summary.csv")

//remove unnecesary columns
val df_HS_SQR_2015_2016_clean = df_HS_SQR_2015_2016_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47","Percent Overage/Undercredited","Enrollment","Percent_Students_With_Disabilities","Percent_English_Language_Learners","Percent_Self_Contained")

//convert proficiency levels to numeric values
val HS_SQR_2015_2016_rdd = df_HS_SQR_2015_2016_clean.rdd.map(line => org.apache.spark.sql.Row(
        2016, //year
        line(0),//dbn
        line(1),//School name
        line(2),//school type
        calculateWeight(List(getSpecRating(line(3)),getSpecRating(line(4)),getSpecRating(line(5)),getSpecRating(line(6)),getSpecRating(line(7)),getSpecRating(line(8)),getSpecRating(line(9)),getQRRating(line(10)),getQRRating(line(11)),convertPercentToScore(line(12)),convertPercentToScore(line(13)),convertPercentToScore(line(14)),convertPercentToScore(line(15)),convertPercentToScore(line(16)),convertPercentToScore(line(17)),getPrincipalScore(line(18)),convertPercentToScore(line(19)),convertPercentToScore(line(20)),convertPercentToScore(line(21)),convertPercentToScore(line(21))))))

//set up schema
val df_HS_SQR_2015_2016_schema = StructType(
        StructField("Year",IntegerType,false)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Weight",DoubleType,true)::Nil)
    

//Create new DF
val df_HS_SQR_2015_2016_ready = sqlContext.createDataFrame(HS_SQR_2015_2016_rdd,df_HS_SQR_2015_2016_schema)

//Create new DF with updated types
val df_HS_SQR_2015_2016_final = df_HS_SQR_2015_2016_ready.selectExpr("Year","DBN",
    "School_Name",
    "School_Type",
    "Weight")



//====== HS_2016_2017 ======
//creates data frame from csv with headers included
val df_HS_SQR_2016_2017_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/HS_SQR_2016_2017_Summary.csv")

//remove unnecesary columns
val df_HS_SQR_2016_2017_clean = df_HS_SQR_2016_2017_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47","Percent Overage/Undercredited","Enrollment","Percent_Students_With_Disabilities","Percent_English_Language_Learners","Percent_Self_Contained")

//convert proficiency levels to numeric values
val HS_SQR_2016_2017_rdd = df_HS_SQR_2016_2017_clean.rdd.map(line => org.apache.spark.sql.Row(
        2017, //year
        line(0),//dbn
        line(1),//School name
        line(2),//school type
        calculateWeight(List(getSpecRating(line(3)),getSpecRating(line(4)),getSpecRating(line(5)),getSpecRating(line(6)),getSpecRating(line(7)),getSpecRating(line(8)),getSpecRating(line(9)),getQRRating(line(10)),getQRRating(line(11)),convertPercentToScore(line(12)),convertPercentToScore(line(13)),convertPercentToScore(line(14)),convertPercentToScore(line(15)),convertPercentToScore(line(16)),convertPercentToScore(line(17)),getPrincipalScore(line(18)),convertPercentToScore(line(19)),convertPercentToScore(line(20)),convertPercentToScore(line(21)),convertPercentToScore(line(21))))))

//set up schema
val df_HS_SQR_2016_2017_schema = StructType(
        StructField("Year",IntegerType,false)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Weight",DoubleType,true)::Nil)
    

//Create new DF
val df_HS_SQR_2016_2017_ready = sqlContext.createDataFrame(HS_SQR_2016_2017_rdd,df_HS_SQR_2016_2017_schema)

//Create new DF with updated types
val df_HS_SQR_2016_2017_final = df_HS_SQR_2016_2017_ready.selectExpr("Year","DBN",
    "School_Name",
    "School_Type",
    "Weight")

//=========== ALL EMS AND HS FOR 2014-2017 ===========
val df_combined_EMS_HS = df_EMS_SQR_2014_2015_final.union(df_EMS_SQR_2015_2016_final).union(df_EMS_SQR_2016_2017_final).union(df_HS_SQR_2014_2015_final).union(df_HS_SQR_2015_2016_final).union(df_HS_SQR_2016_2017_final)







