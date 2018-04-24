//Inspiration from https://stackoverflow.com/questions/44516627/how-to-convert-a-dataframe-to-dataset-in-apache-spark-in-scala
//Inspiration from https://stackoverflow.com/questions/37851606/how-to-join-datasets-on-multiple-columns

val dfsl = df_combined_School_Locations.select("Year2","DBN2","Latitude","Longitude")
val dfsemshs = df_combined_EMS_HS.select("*")

case class schoollocs(Year2:  Int, DBN2:  String, Latitude: Double, Longitude:Double)
val ds2:Dataset[schoollocs] = dfsl.as[schoollocs]


case class schooldata(Year: Int, DBN: String, School_Name: String, School_Type: String, Enrollment: Int, Rigorous_Instruction_Rating: Int, Collaborative_Teachers_Rating: Int, Supportive_Environment_Rating: Int, Effective_School_Leadership_Rating: Int, Strong_Family_Community_Ties_Rating: Int, Trust_Rating: Int, Student_Achievement_Rating: Int, Quality_Review_Effective_Teaching_Learning: Int, Quality_Review_Effective_School_Assessment: Int, Percent_English_Language_Learners: Float, Percent_Students_With_Disabilities: Float, Percent_Self_Contained: Float, Economic_Need_Index: Float, Percent_In_Temp_Housing: Float, Percent_HRA_Eligible: Float, Percent_Teachers_With_Three_Or_More_Years: Float, Student_Attendance_Rate: Float, Percent_Students_Chronically_Absent: Float, Teacher_Attendance_Rate: Float)
val ds1:Dataset[schooldata] = dfsemshs.as[schooldata]


val schoolswithgps = ds1.join(ds2).where(ds1("Year") === ds2("Year2")).where(ds1("DBN") === ds2("DBN2")).drop("Year2","DBN2")


