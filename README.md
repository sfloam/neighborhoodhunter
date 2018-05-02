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

##### B. Modify HDFS paths in src/housing/Recent-And-2017-Sales.scala
##### B. Modify HDFS paths in src/housing/Historical-Housing.scala

##### C. Enter into command line
```module load spark
spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 
```

##To Convert Model Result to D3 Compatible GeoJSON
##### Run the RebuildJSON.java with two input files 
- model result: resources/model/Output_H.0.35_C.0.4_S.0.25.csv )
- NYC GeoJSON: resources/nyc.geojson
- Result: resources/model/JsonBuilderResult.json

## To view map of data follow these steps:
1. In your command line:
- A. python -m SimpleHTTPServer
- B. open index

## To interact with the map:
1. Hover over a neighborhood to view the name of that neighborhood
2. Click on a datapoint to view information about that data point
- A. Currently the map is set up to display school locations in NYC. This will be converted into data relating to our model.
- B. If variance is greater than zero, the color of the dot will stay red. If variance is less than zero, the color of the dot will turn blue when you hover over it.
- C. This demonstration is meant to show a mock up of the functionality we plan to implement in our application

## Recent Updates
1. School Data now accounts for variance and generates GPS locations of each school
2. Crime data is updated with variance and sorted based on variance values
3. School Data now can generate geojson code used for plotting information
- A. This code will be repurposed later for our model code



