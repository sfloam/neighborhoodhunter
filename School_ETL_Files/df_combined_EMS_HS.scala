
//combine data
val df_combined_EMS_HS = df_EMS_SQR_2014_2015_final.union(df_EMS_SQR_2015_2016_final).union(df_EMS_SQR_2016_2017_final).union(df_HS_SQR_2014_2015_final).union(df_HS_SQR_2015_2016_final).union(df_HS_SQR_2016_2017_final)

//Begin Profiling

//get number of rows: 5260
df_combined_EMS_HS.count()

//check columns
df_combined_EMS_HS.columns
/*Array[String] = Array(Year, DBN, School_Name, School_Type, Enrollment, Rigorous_Instruction_Rating, Collaborative_Teachers_Rating, Supportive_Environment_Rating, Effective_School_Leadership_Rating, Strong_Family_Community_Ties_Rating, Trust_Rating, Student_Achievement_Rating, Quality_Review_Effective_Teaching_Learning, Quality_Review_Effective_School_Assessment, Percent_English_Language_Learners, Percent_Students_With_Disabilities, Percent_Self_Contained, Economic_Need_Index, Percent_In_Temp_Housing, Percent_HRA_Eligible, Years_Principal_Experience_At_School, Percent_Teachers_With_Three_Or_More_Years, Student_Attendance_Rate, Percent_Students_Chronically_Absent, Teacher_Attendance_Rate)*/

//counts number of columns: 25
df_combined_EMS_HS.columns.size

//confirm no incorrect years
df_combined_EMS_HS.select("Year","School_Name").where("Year != 2014 AND Year != 2015 AND Year != 2016").show

//confirm all schools have a DBN
df_combined_EMS_HS.select("DBN","School_Name").where("DBN is null").show

//confirm all schools have a Name
df_combined_EMS_HS.select("DBN","School_Name").where("School_Name is null").show

//idea came from https://stackoverflow.com/questions/20285209/find-min-and-max-elements-of-array
//largest school name = size of 50
df_combined_EMS_HS.rdd.map(line=>line(2).toString.size).reduce(_ max _)

//show 2016 student achievement: 5260 for testing purposes
df_combined_EMS_HS.select("year","School_Name","Enrollment","Student_Achievement_Rating").where("year=2016 and Student_Achievement_Rating is not null").sort(df_combined_EMS_HS("Student_Achievement_Rating").asc).show(5260)

//Show Averages  All School_Types
df_combined_EMS_HS.groupBy($"School_Type").avg("Enrollment","Student_Attendance_Rate","Percent_Students_Chronically_Absent","Teacher_Attendance_Rate","Percent_Teachers_With_Three_Or_More_Years","Years_Principal_Experience_At_School","Economic_Need_Index","Percent_In_Temp_Housing","Percent_HRA_Eligible").show()

//Get top 5 schools with highest enrollment
df_combined_EMS_HS.select("School_Name","Enrollment").sort(df_combined_EMS_HS("Enrollment").desc).limit(5).show

//Get top 5 schools with lowest enrollment
df_combined_EMS_HS.select("School_Name","Enrollment").sort(df_combined_EMS_HS("Enrollment").asc).limit(5).show

//Get top 5 schools with highest Economic_Need_Index in 2016
df_combined_EMS_HS.select("Year","School_Name","Economic_Need_Index").where("Economic_Need_Index is not null AND Year=2016").sort(df_combined_EMS_HS("Economic_Need_Index").desc).limit(5).show

//Get top 5 schools with highest Percent_In_Temp_Housing in 2016
df_combined_EMS_HS.select("School_Name","Percent_In_Temp_Housing").where("Percent_In_Temp_Housing is not null AND Year=2016").sort(df_combined_EMS_HS("Percent_In_Temp_Housing").desc).limit(5).show





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