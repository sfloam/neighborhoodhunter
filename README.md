# Neighborhoods

## On DUMBO HPC:
## To Generate School Data in Spark:

##### A. Load school data into HDFS
- SCHOOL_LOCATIONS_2014_2015_JSON.json
- SCHOOL_LOCATIONS_2015_2016_JSON.json
- SCHOOL_LOCATIONS_2016_2017_JSON.json
- HS_SQR_2014_2015_Summary.csv
- HS_SQR_2015_2016_Summary.csv
- HS_SQR_2016_2017_Summary.csv
- EMS_SQR_2014_2015_Summary.csv
- EMS_SQR_2015_2016_Summary.csv
- EMS_SQR_2016_2017_Summary.csv

##### B. Modify HDFS paths in School_Locations.scala, Updated_EMS_HS.scala, Updated_Join_Latitude_Longitude.scala

##### C. Enter into command line
```module load spark
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
:load School_Locations.scala
:load Updated_EMS_HS.scala
:load Updated_Join_Latitude_Longitude.scala
```

## To Generate Crime Data in Spark:
##### A. Load crime data into HDFS
- resources/crime/RawData/misdemeanor-offenses-by-precinct-2000-2017.csv
- resources/crime/RawData/non-seven-major-felony-offenses-by-precinct-2000-2017.csv
- resources/crime/RawData/seven-major-felony-offenses-by-precinct-2000-2017.csv
- resources/crime/RawData/violation-offenses-by-precinct-2000-2017.csv

##### B. Modify HDFS paths in src/crime/CrimeDataETL.scala

##### C. Enter into command line
```module load spark
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
:load CrimeDataETL.scala
```
## To Generate Housing Data in Spark:
##### A. Load housing data into HDFS
  
  All files are in resources/housing/rawData
  
  or from Dumbo
- /user/sc2936/housingSalesRaw/rollingsales_bronx.csv
- /user/sc2936/housingSalesRaw/rollingsales_brooklyn.csv
- /user/sc2936/housingSalesRaw/rollingsales_manhattan.csv
- /user/sc2936/housingSalesRaw/rollingsales_queens.csv
- /user/sc2936/housingSalesRaw/rollingsales_statenisland.csv

These last five folders for "borough"_sales_prices each contain seperate files of data per year 2005-2016
- /user/sc2936/housingSalesRaw/bronx_sales_prices
- /user/sc2936/housingSalesRaw/brooklyn_sales_prices
- /user/sc2936/housingSalesRaw/manhattan_sales_prices
- /user/sc2936/housingSalesRaw/queens_sales_prices
- /user/sc2936/housingSalesRaw/staten_island_sales_prices

##### B.1 Modify HDFS paths in src/housing/Recent-And-2017-Sales.scala
 - You must run Recent-And-2017-Sales.scala first as it will generate the input file "housingSalesClean/new2017_5.1.18" for Historical-Housing.scala
  - It will also generate "summary_2017_2018_5.1.2018", which is an input of recent sale prices for the model
##### B.2 Modify HDFS paths in src/housing/Historical-Housing.scala
  - Historical-Housing.scala will generate one of the input files for the model "housingSalesClean/historical_all_buildingType_5.1.18"
  
##### C. Enter into command line
```module load spark
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
:load Recent-And-2017-Sales.scala
:load Historical-Housing.scala
```

## To Run the model in Spark:
##### A. Load output Crime, School, and Housing data from above into HDFS
output files can be found at:
- resources/housing/historical_all_buildingType_5.1.18.csv
- resources/housing/summary_2017_2018_5.2.2018
- resources/crime/CrimeWithPrediction.csv
- resources/school/Final_Output/NYC_School_Data.csv

##### B. Modify HDFS paths in src/Model_PoC/Model.scala
    The output of the model will generate the input for the geo json builder -> resources/model/Output_H.0.35_C.0.4_S.0.25.csv
    
    The weights for the model are hard coded within the file and changed for each iteration.
    The best wieghts are currently in use:
    val housing_weight = .35
    val crime_weight = .4
    val school_weight = .25
    
##### C. Enter into command line
```module load spark
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
:load Model.scala
```
## To Convert Model Result to D3 Compatible GeoJSON
##### Run the RebuildJSON.java with two input files 
- model result: resources/model/Output_H.0.35_C.0.4_S.0.25.csv )
- NYC GeoJSON: resources/nyc.geojson
- Result: resources/model/JsonBuilderResult.json

## To view map of data follow these steps:
1. In your command line:
- python -m SimpleHTTPServer
- open index.html or go to 0.0.0.0:8000

## To interact with the map:
1. Hover over a neighborhood to view the name of that neighborhood
2. Select your budget and home type
- The red areas reveal the top five neighborhoods that are about to pop with respect to your budget and home needs (some budget ranges will reveal fewer than five)
- These areas will update as you change your budget and home type
- If you would like to see the data relating to the top five ranks, view the console log.
