//module load spark

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
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType};

//creates data frame from csv with headers included
val df_HS_SQR_2014_2015_Summary = sqlContext.read.format("csv").option("header", "true").load("hdfs:///user/smf463/bdad-project/educationData/HS_SQR_2014_2015_Summary.csv")

//removes uncessary columns
val df_HS_SQR_2014_2015_clean = df_HS_SQR_2014_2015_Summary.drop("Rigorous Instruction - Percent Positive","Collaborative Teachers - Percent Positive","Supportive Environment - Percent Positive", "Effective School Leadership - Percent Positive", "Strong Family-Community Ties - Percent Positive", "Trust - Percent Positive","Quality Review - How interesting and challenging is the curriculum?","Quality Review - How well do teachers work with each other?","Quality Review - How clearly are high expectations communicated to students and staff?","Quality Review - Dates of Review","Average Grade 4 English Proficiency","Average Grade 4 Math Proficiency","Percent Asian","Percent Black","Percent Hispanic","Percent White","Average Grade 5 English Proficiency","Average Grade 5 Math Proficiency","Quality Review - How safe and inclusive is the school while supporting social-emotional growth?","Quality Review - How well does the school allocate and manage resources?","Quality Review - How well does the school identify, track, and meet its goals?","Quality Review - How thoughtful is the school’s approach to teacher development and evaluation?","Quality Review - How well are school decisions evaluated and adjusted?","Average Incoming ELA Proficiency (Based on 4th Grade)","Average Incoming Math Proficiency (Based on 4th Grade)","Average Grade 8 English Proficiency","Average Grade 8 Math Proficiency","_c40","_c41","_c42","_c43","_c44","_c45","_c46","_c47")

//create rdd for data cleaning (adds years, formats ratings into numeric values)
val HS_SQR_2014_2015_rdd = df_HS_SQR_2014_2015_clean.rdd.map(line => org.apache.spark.sql.Row(
        2014,
        line(0),
        line(1),
        line(2),
        line(3),
        if(line(4) == "Meeting Target"){3} 
        else if(line(4) == "Approaching Target"){2}
        else if(line(4) == "Exceeding Target"){4} 
        else if(line(4) == "Not Meeting Target"){1} 
        else{null},
        if(line(5) == "Meeting Target"){3} 
        else if(line(5) == "Approaching Target"){2} 
        else if(line(5) == "Exceeding Target"){4} 
        else if(line(5) == "Not Meeting Target"){1} 
        else{null},
        if(line(6) == "Meeting Target"){3} 
        else if(line(6) == "Approaching Target"){2}
        else if(line(6) == "Exceeding Target"){4} 
        else if(line(6) == "Not Meeting Target"){1} 
        else{null},if(line(7) == "Meeting Target"){3} 
        else if(line(7) == "Approaching Target"){2} 
        else if(line(7) == "Exceeding Target"){4} 
        else if(line(7) == "Not Meeting Target"){1} 
        else{null},
        if(line(8) == "Meeting Target"){3}
        else if(line(8) == "Approaching Target"){2} 
        else if(line(8) == "Exceeding Target"){4} 
        else if(line(8) == "Not Meeting Target"){1} 
        else{null},
        if(line(9) == "Meeting Target"){3} 
        else if(line(9) == "Approaching Target"){2} 
        else if(line(9) == "Exceeding Target"){4} 
        else if(line(9) == "Not Meeting Target"){1} 
        else{null},if(line(10) == "Meeting Target"){3} 
        else if(line(10) == "Approaching Target"){2} 
        else if(line(10) == "Exceeding Target"){4} 
        else if(line(10) == "Not Meeting Target"){1} 
        else{null},
        if(line(11) == "Developing"){2} 
        else if(line(11) == "Proficient"){3} 
        else if(line(11) == "Well Developed"){4} 
        else if(line(11) == "Under Developed"){1} 
        else{null},if(line(12) == "Developing"){2} 
        else if(line(12) == "Proficient"){3} 
        else if(line(12) == "Well Developed"){4} 
        else if(line(12) == "Under Developed"){1} 
        else{null},
        line(13),
        line(14),
        line(15),
        line(16),
        line(17),
        line(18),
        line(19),
        line(20),
        line(21),
        line(22),
        line(23)))

//revise schema
val df_HS_SQR_2014_2015_schema = StructType(
        StructField("Year",IntegerType,true)::
        StructField("DBN",StringType,true)::
        StructField("School_Name",StringType,true)::
        StructField("School_Type",StringType,true)::
        StructField("Enrollment",StringType,true)::
        StructField("Rigorous_Instruction_Rating",IntegerType,true)::
        StructField("Collaborative_Teachers_Rating",IntegerType,true)::
        StructField("Supportive_Environment_Rating",IntegerType,true)::
        StructField("Effective_School_Leadership_Rating",IntegerType,true)::
        StructField("Strong_Family_Community_Ties_Rating",IntegerType,true)::
        StructField("Trust_Rating",IntegerType,true)::
        StructField("Student_Achievement_Rating",IntegerType,true)::
        StructField("Quality_Review_Effective_Teaching_Learning",IntegerType,true)::
        StructField("Quality_Review_Effective_School_Assessment",IntegerType,true)::
        StructField("Percent_English_Language_Learners",StringType,true)::
        StructField("Percent_Students_With_Disabilities",StringType,true):: 
        StructField("Percent_Self_Contained",StringType,true)::
        StructField("Economic_Need_Index",StringType,true)::
        StructField("Percent_In_Temp_Housing",StringType,true)::
        StructField("Percent_HRA_Eligible",StringType,true)::
        StructField("Years_Principal_Experience_At_School",StringType,true)::
        StructField("Percent_Teachers_With_Three_Or_More_Years",StringType,true)::
        StructField("Student_Attendance_Rate",StringType,true)::
        StructField("Percent_Students_Chronically_Absent",StringType,true)::
        StructField("Teacher_Attendance_Rate",StringType,true)::Nil)

//create new dataframe with new schema and cleaned data
val df_HS_SQR_2014_2015_ready = sqlContext.createDataFrame(HS_SQR_2014_2015_rdd,df_HS_SQR_2014_2015_schema)

//modify types of columns
val df_HS_SQR_2014_2015_final = df_HS_SQR_2014_2015_ready.selectExpr("Year",
    "DBN",
    "School_Name",
    "School_Type",
    "cast(Enrollment as int) Enrollment",
    "Rigorous_Instruction_Rating",
    "Collaborative_Teachers_Rating",
    "Supportive_Environment_Rating",
    "Effective_School_Leadership_Rating",
    "Strong_Family_Community_Ties_Rating",
    "Trust_Rating",
    "Student_Achievement_Rating",
    "Quality_Review_Effective_Teaching_Learning",
    "Quality_Review_Effective_School_Assessment",
    "cast(Percent_English_Language_Learners as float) Percent_English_Language_Learners",
    "cast(Percent_Students_With_Disabilities as float) Percent_Students_With_Disabilities",
    "cast(Percent_Self_Contained as float) Percent_Self_Contained",
    "cast(Economic_Need_Index as float) Economic_Need_Index",
    "cast(Percent_In_Temp_Housing as float) Percent_In_Temp_Housing",
    "cast(Percent_HRA_Eligible as float) Percent_HRA_Eligible",
    "cast(Years_Principal_Experience_At_School as float) Years_Principal_Experience_At_School",
    "cast(Percent_Teachers_With_Three_Or_More_Years as float) Percent_Teachers_With_Three_Or_More_Years",
    "cast(Student_Attendance_Rate as float) Student_Attendance_Rate",
    "cast(Percent_Students_Chronically_Absent as float) Percent_Students_Chronically_Absent",
    "cast(Teacher_Attendance_Rate as float) Teacher_Attendance_Rate")

//Column Names
/*  "Year",
    "DBN",
    "School_Name",
    "School_Type",
    "cast(Enrollment as int) Enrollment",
    "Rigorous_Instruction_Rating",
    "Collaborative_Teachers_Rating",
    "Supportive_Environment_Rating",
    "Effective_School_Leadership_Rating",
    "Strong_Family_Community_Ties_Rating",
    "Trust_Rating",
    "Student_Achievement_Rating",
    "Quality_Review_Effective_Teaching_Learning",
    "Quality_Review_Effective_School_Assessment",
    "cast(Percent_English_Language_Learners as float) Percent_English_Language_Learners",
    "cast(Percent_Students_With_Disabilities as float) Percent_Students_With_Disabilities",
    "cast(Percent_Self_Contained as float) Percent_Self_Contained",
    "cast(Economic_Need_Index as float) Economic_Need_Index",
    "cast(Percent_In_Temp_Housing as float) Percent_In_Temp_Housing",
    "cast(Percent_HRA_Eligible as float) Percent_HRA_Eligible",
    "cast(Years_Principal_Experience_At_School as float) Years_Principal_Experience_At_School",
    "cast(Percent_Teachers_With_Three_Or_More_Years as float) Percent_Teachers_With_Three_Or_More_Years",
    "cast(Student_Attendance_Rate as float) Student_Attendance_Rate",
    "cast(Percent_Students_Chronically_Absent as float) Percent_Students_Chronically_Absent",
    "cast(Teacher_Attendance_Rate as float) Teacher_Attendance_Rate"*/