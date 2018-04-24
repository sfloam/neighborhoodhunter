//imports
    //spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 

    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.apache.spark.SparkConf 
    import scala.collection.mutable.ArrayBuffer 
    import org.apache.spark.sql.SQLContext 
    import sqlContext._
    import sqlContext.implicits._ 
    import org.apache.spark.sql.functions._

    val sqlContext = new SQLContext(sc) 


    val dfMH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/rollingsales_manhattan.csv").withColumn("BOROUGH", lit("MANHATTAN"))
    val dfBronx = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/rollingsales_bronx.csv").withColumn("BOROUGH", lit("BRONX"))
    val dfQueens = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/rollingsales_queens.csv").withColumn("BOROUGH", lit("QUEENS"))
    val dfBK = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/rollingsales_brooklyn.csv").withColumn("BOROUGH", lit("BROOKLYN"))
    val dfSI = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/rollingsales_statenisland.csv").withColumn("BOROUGH", lit("STATEN ISLAND"))


    // join all of the files
    val mergedDF = dfMH.unionAll(dfBronx).unionAll(dfQueens).unionAll(dfBK).unionAll(dfSI)


    // clean data
    // remove white spaces from the column names which we will use
    val dfColsRename = mergedDF.withColumnRenamed("BUILDING CLASS CATEGORY", "BUILDING_CLASS_CATEGORY").withColumnRenamed("SALE PRICE", "SALE_PRICE").withColumnRenamed("SALE DATE", "SALE_DATE")

    //select deisred columns and filter out "$"" and "," out of sales value so it can later be turned into a string       
    val dfColsSelect = dfColsRename.selectExpr("NEIGHBORHOOD", "BOROUGH", "BUILDING_CLASS_CATEGORY", "translate(SALE_PRICE, '$|,|-', '') SALE_PRICE", "SALE_DATE")

    // cast string sales price as an double
    val dfDouble = dfColsSelect.selectExpr("NEIGHBORHOOD", "BOROUGH", "BUILDING_CLASS_CATEGORY", "cast(SALE_PRICE as Double) SALE_PRICE", "SALE_DATE")

    //remove low value sales prices such as -$0, $0, $10, $100,000 (these tend to not be typical transactions)
    // transactions that are <$100,000 tend to be for listings which have special requirements of the buyer (like certain income restrictions) 
    // and are nto indicative of the market. This value may need some tinkering.
    val dfSaleNot0 = dfDouble.filter($"SALE_PRICE" > 100000)

    //filter the data for sale types that are relevant. Example not including commercial sales, rental sales, parking spot sales etc
    val filtDF = dfSaleNot0 .filter(($"BUILDING_CLASS_CATEGORY" like "%09 COOPS - WALKUP APARTMENTS%") || 
    ($"BUILDING_CLASS_CATEGORY" like "%10 COOPS - ELEVATOR APARTMENTS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%12 CONDOS - WALKUP APARTMENTS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%13 CONDOS - ELEVATOR APARTMENTS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%01 ONE FAMILY DWELLINGS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%02 TWO FAMILY DWELLINGS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%03 THREE FAMILY DWELLINGS%") )



    //update neighborhood names to match group naming
    // read the neighborhood naming structure which we will merge on 
    val names = sqlContext.read.format("csv").option("header", "true").load("housingNameConvention/NHoodNameCentroids.csv")
    val namesList = names.select("Name").rdd.map(r => r(0)).collect.toList


    //clean neighborhood names
    // some of the neighborhoods are combined by a '/', split them out and keep the same stats for each
    //filter outs the extra null columns that this creates at the end
    // change all neighborhood names to camel case
    //rename certain neighborhoods which I could identify as having the wrong name.
    //need to eralign the neighborhood names to the new naming heirarchy to work with D3
    


   
    // val nameUpdate1 = nhFormat.withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Jamaica", "Jamaica Center").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Univ Hts", "University Heights").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Westchester", "Westchester Square").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Flatbush-lefferts Garden", "Prospect Lefferts Gardens").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Flatbush-east", " East Flatbush").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Flatbush-central", "Flatbush").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Flatbush-north", "Flatbush").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Seagate", "Sea Gate").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Harlem-central", "Central Harlem").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Harlem-east", "East Harlem").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Harlem-upper", "Harlem").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Harlem-west", "Harlem").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Financial", "Financial District").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Greenwich Village-west", "West Village").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Greenwich Village-central", "Greenwich Village").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Great Kills-bay Terrace", "Bay Terrace").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Mariners Harbor", "Mariner's Harbor").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "New Brighton-st. George", "St. George").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "West New Brighton", "West Brighton").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Dongan Hills-old Town", "Old Town").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "New Dorp-beach", "New Dorp Beach").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Princes Bay", "Prince's Bay").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Richmondtown", "Richmond Town").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Williamsburg-east", "East Williamsburg").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Williamsburg-north", "Williamsburg").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Williamsburg-south", "Williamsburg").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Arrochar-shore Acres", "Shore Acres").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Pelham Parkway North", "Pelham Parkway").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Pelham Parkway South", "Pelham Parkway").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Ocean Parkway-north", "Ocean Parkway").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Ocean Parkway-south", "Ocean Parkway").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Ocean Parkway-south", "Ocean Parkway").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Midtown East", "Midtown").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Midtown West", "Midtown").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Midtown Cbd", "Midtown").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper East Side (59-79)", "Upper East Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper East Side (79-96)", "Upper East Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper East Side (96-110)", "Upper East Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper West Side (59-79)", "Upper West Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper West Side (59-79)", "Upper West Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper West Side (79-96)", "Upper West Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Upper West Side (96-116)", "Upper West Side").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Washington Heights Lower", "Washington Heights").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Washington Heights Upper", "Washington Heights").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Flushing-north", "Downtown Flushing").otherwise(col("NEIGHBORHOOD_FORMAT"))).
    //     withColumn("NEIGHBORHOOD_FORMAT", when(col("NEIGHBORHOOD_FORMAT") === "Flushing-south", "Downtown Flushing").otherwise(col("NEIGHBORHOOD_FORMAT")))

    

//     val nameUpdate1 = nhFormat.withColumn("new_name", when(col("NEIGHBORHOOD_FORMAT") === "Jamaica", "Jamaica Center").otherwise(col("NEIGHBORHOOD_FORMAT")))
//     val nameUpdate2 = nameUpdate1.select($"new_name", regexp_replace($"new_name","Highbridge","High Bridge"), 
//     regexp_replace($"new_name","Univ Hts","University Heights"),
//     regexp_replace($"new_name","Westchester","Westchester Square"),
//     regexp_replace($"new_name","Flatbush-lefferts Garden","Prospect Lefferts Gardens"),
//     regexp_replace($"new_name","Flatbush-east","East Flatbush"),
//     regexp_replace($"new_name","Flatbush-central","Flatbush"),
//     regexp_replace($"new_name","Seagate","Sea Gate"),
//     regexp_replace($"new_name","Harlem-central","Central Harlem"),
//     regexp_replace($"new_name","Harlem-east","East Harlem"),
//     regexp_replace($"new_name","Harlem-upper","Harlem"),
//     regexp_replace($"new_name","Harlem-west","Harlem"),
//     regexp_replace($"new_name","Financial","Financial District"),
//     regexp_replace($"new_name","Greenwich Village-west","West Village"),
//     regexp_replace($"new_name","Greenwich Village-central","Greenwich Village"),
//     regexp_replace($"new_name","Great Kills-bay Terrace","Bay Terrace"),
//     regexp_replace($"new_name","Mariners Harbor","Mariner's Harbor"),
//     regexp_replace($"new_name","New Brighton-st. George","St. George"),
//     regexp_replace($"new_name","West New Brighton","West Brighton"),
//     regexp_replace($"new_name","Dongan Hills-old Town","Old Town"),
//     regexp_replace($"new_name","New Dorp-beach","New Dorp Beach"),
//     regexp_replace($"new_name","Princes Bay","Prince's Bay"),
//     regexp_replace($"new_name","Richmondtown","Richmond Town"),
//     regexp_replace($"new_name","Williamsburg-east","East Williamsburg"),
//     regexp_replace($"new_name","Arrochar-shore Acres","Shore Acres"),
//     regexp_replace($"new_name","Arrochar-shore Acres","Shore Acres"),
//     regexp_replace($"new_name","Midtown East","Turtle Bay"),
//     regexp_replace($"new_name","Pelham Parkway North","Pelham Parkway"),
//     regexp_replace($"new_name","Pelham Parkway South","Pelham Parkway"),
//     regexp_replace($"new_name","Ocean Parkway-north","Ocean Parkway"),
//     regexp_replace($"new_name","Ocean Parkway-south","Ocean Parkway"),
//     regexp_replace($"new_name","Williamsburg-north","Williamsburg"),
//     regexp_replace($"new_name","Williamsburg-south","Williamsburg"),
//     regexp_replace($"new_name","Midtown Cbd","Midtown"),
//     regexp_replace($"new_name","Midtown East","Midtown"),
//     regexp_replace($"new_name","Midtown West","Midtown"),
//     regexp_replace($"new_name","Upper East Side (59-79)","Upper East Side"),
//     regexp_replace($"new_name","Upper East Side (79-96)","Upper East Side"),
//     regexp_replace($"new_name","Upper East Side (96-110)","Upper East Side"),
//     regexp_replace($"new_name","Upper West Side (59-79)","Upper West Side"),
//     regexp_replace($"new_name","Upper West Side (79-96)","Upper West Side"),
//     regexp_replace($"new_name","Upper West Side (96-116)","Upper West Side"),
//     regexp_replace($"new_name","Washington Heights Lower","Washington Heights"),
//     regexp_replace($"new_name","Washington Heights Upper","Washington Heights"),
//     regexp_replace($"new_name","Flushing-north","Downtown Flushing"),
//     regexp_replace($"new_name","Flushing-south","Downtown Flushing"),
//     regexp_replace($"new_name","Flushing-south","Downtown Flushing") 
// )

    ////can llok into adding this granularity in
    //regexp_replace($"new_name","Upper East Side (79-96)","Yorkville")
   // regexp_replace($"new_name","Upper East Side (59-79)","Lenox Hill"),
   // regexp_replace($"new_name","Upper West Side (59-79)","Lincoln Square"),


    //filter results for matching neighborhood names, review neighborhoods which don't match
    val mismatch = nhFormat.filter(!($"NEIGHBORHOOD_FORMAT".isin(namesList:_*))  )
    mismatch.write.option("header", "true").format("csv").save("/user/sc2936/housingNamingConvention/mismatch2")

    val matching = nhFormat.filter(($"NEIGHBORHOOD_FORMAT".isin(namesList:_*)) && !($"NEIGHBORHOOD" == null))



    //group the data
    // calculate the min sale price for each group, the max sale price for each group as well as the total number of sales
    val groupedDF = matching.groupBy($"NEIGHBORHOOD_FORMAT", $"BOROUGH",$"BUILDING_CLASS_CATEGORY").agg(min("SALE_PRICE") as "MIN_2017_2018", max("SALE_PRICE") as "MAX_2017_2018", mean("SALE_PRICE") as "MEAN_2017_2018", count("SALE_PRICE") as "SALE_COUNT").orderBy($"BOROUGH", $"NEIGHBORHOOD_FORMAT", $"BUILDING_CLASS_CATEGORY")


    //save the file for 2017-2018 summary
    groupedDF.write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/summary2017_2018")


    //show the schema
    groupedDF.schema


    //test re-reading the data and that column names are the same
    val testRead = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/summary2017_2018")
    testRead.limit(5).show
    testRead.count == groupedDF.count




// extract 2017 data only , include less building category types for data merge

  val yearDF = dfDouble.withColumn("SALE_YEAR", split($"SALE_DATE", "/")(2)).drop($"SALE_DATE")
  val yearDFmin = yearDF.filter($"SALE_PRICE" > 100000)


  val filtYearDF = yearDFmin.filter((($"BUILDING_CLASS_CATEGORY" like "%01 ONE FAMILY DWELLINGS%") ||
  ($"BUILDING_CLASS_CATEGORY" like "%02 TWO FAMILY DWELLINGS%") ||
  ($"BUILDING_CLASS_CATEGORY" like "%03 THREE FAMILY DWELLINGS%")) &&
  ($"SALE_YEAR" like "%2017%") )


  //group the data
  // calculate the min sale price for each group, the max sale price for each group as well as the total number of sales
  val calc2017DF = filtYearDF.groupBy($"NEIGHBORHOOD", $"BOROUGH", $"SALE_YEAR", $"BUILDING_CLASS_CATEGORY").agg(count("SALE_PRICE") as "NUMBER_OF_SALES", mean("SALE_PRICE") as "AVERAGE_PRICE").orderBy($"BOROUGH", $"NEIGHBORHOOD", $"BUILDING_CLASS_CATEGORY")

  //save the 2017 file
  calc2017DF.write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/summary2017")


  









