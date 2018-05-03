//This program creates summaries for 2017 and recent NYC housing sales from raw data.
//The output of this program must be used as input to the Historical-Housing class, 
//which formats housing data from all years which will be included in the model

    // spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 

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
    val mergedDF = dfMH.unionAll(dfBronx).unionAll(dfQueens).unionAll(dfBK).unionAll(dfSI)


    // clean data
    // remove white spaces from the column names which we will use
    val dfColsRename = mergedDF.withColumnRenamed("BUILDING CLASS CATEGORY", "BUILDING_CLASS_CATEGORY").withColumnRenamed("SALE PRICE", "SALE_PRICE").withColumnRenamed("SALE DATE", "SALE_DATE")

    //Filter out "$"" and "," out of sales value so it can later be turned into an int      
    val dfColsSelect = dfColsRename.selectExpr("NEIGHBORHOOD", "BOROUGH", "BUILDING_CLASS_CATEGORY", "translate(SALE_PRICE, '$|,|-', '') SALE_PRICE", "SALE_DATE")

    //cast string sales price as an double
    val dfDouble = dfColsSelect.selectExpr("NEIGHBORHOOD", "BOROUGH", "BUILDING_CLASS_CATEGORY", "cast(SALE_PRICE as Double) SALE_PRICE", "SALE_DATE")

    //remove low value sales prices such as -$0, $0, $10, $100,000 (these tend to not be typical transactions)
    // transactions that are <$100,000 tend to be for listings which have special requirements of the buyer (like certain income restrictions) 
    // and are not indicative of the market. 
    val dfSaleNot0 = dfDouble.filter($"SALE_PRICE" > 100000)

    //filter the data for sale types that are relevant. Example not including commercial sales, rental sales, parking spot sales etc
    val filtDF = dfSaleNot0 .filter(($"BUILDING_CLASS_CATEGORY" like "%09 COOPS - WALKUP APARTMENTS%") || 
    ($"BUILDING_CLASS_CATEGORY" like "%10 COOPS - ELEVATOR APARTMENTS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%12 CONDOS - WALKUP APARTMENTS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%13 CONDOS - ELEVATOR APARTMENTS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%01 ONE FAMILY DWELLINGS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%02 TWO FAMILY DWELLINGS%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%03 THREE FAMILY DWELLINGS%") )



   
    // read the neighborhood naming structure which we will merge on 
    // reformat neighborhood names to match naming convention in the lookup table
    // some of the neighborhoods are combined by a '/', split them out and keep the same stats for each
    // change all neighborhood names to camel case
    // rename certain neighborhoods which I can identify as having the wrong name.
    
   
    val names = sqlContext.read.format("csv").option("header", "true").load("housingNameConvention/LookupTable.csv")
    val namesList = names.select("Name").rdd.map(r => r(0)).collect.toList

    val nhNameSPlit1 = filtDF.withColumn("NEIGHBORHOOD", split($"NEIGHBORHOOD", "/")(0))
    val nhNameSPlit12 = filtDF.withColumn("NEIGHBORHOOD", split($"NEIGHBORHOOD", "/")(1)) 
    val mergedNHnames = nhNameSPlit1.unionAll(nhNameSPlit12)
    val nhFormatLower = mergedNHnames.withColumn("NEIGHBORHOOD", lower(col("NEIGHBORHOOD")))
    val nhFormatCamel = nhFormatLower.select(initcap($"NEIGHBORHOOD") as "NEIGHBORHOOD", 
        $"BOROUGH", $"BUILDING_CLASS_CATEGORY", $"SALE_PRICE", $"SALE_DATE" ).
        withColumnRenamed("SALE DATE", "SALE_DATE")

    
     val nhNamesUpdated = nhFormatCamel.withColumn("NEIGHBORHOOD", 
            when(col("NEIGHBORHOOD") === "East Tremont", "Tremont")
            .when(col("NEIGHBORHOOD") === "Univ Hts", "University Heights")
            .when(col("NEIGHBORHOOD") === "Westchester", "Westchester Square")
            .when(col("NEIGHBORHOOD") === "Bedford Stuyvesant", "Bedford-Stuyvesant")
            .when(col("NEIGHBORHOOD") === "Cobble Hill-west", "Cobble Hill")
            .when(col("NEIGHBORHOOD") === "Downtown-fulton Ferry", "Downtown Brooklyn")
            .when(col("NEIGHBORHOOD") === "Downtown-fulton Mall", "Downtown Brooklyn")
            .when(col("NEIGHBORHOOD") === "Downtown-metrotech", "Downtown Brooklyn")
            .when(col("NEIGHBORHOOD") === "Flatbush-central", "Flatbush")
            .when(col("NEIGHBORHOOD") === "Flatbush-east", "East Flatbush")
            .when(col("NEIGHBORHOOD") === "Flatbush-lefferts Garden", "Prospect-Lefferts Gardens")
            .when(col("NEIGHBORHOOD") === "Flatbush-north", "Flatbush")
            .when(col("NEIGHBORHOOD") === "Williamsburg-central", "Williamsburg")
            .when(col("NEIGHBORHOOD") === "Williamsburg-east", "Williamsburg")
            .when(col("NEIGHBORHOOD") === "Williamsburg-north", "Williamsburg")
            .when(col("NEIGHBORHOOD") === "Williamsburg-south", "Williamsburg")
            .when(col("NEIGHBORHOOD") === "Seagate", "Sea Gate")
            .when(col("NEIGHBORHOOD") === "Park Slope South", "South Slope")
            .when(col("NEIGHBORHOOD") === "Financial", "Financial District")
            .when(col("NEIGHBORHOOD") === "Flatiron", "Flatiron District")
            .when(col("NEIGHBORHOOD") === "Greenwich Village-central", "Greenwich Village")
            .when(col("NEIGHBORHOOD") === "Greenwich Village-west", "West Village")
            .when(col("NEIGHBORHOOD") === "Harlem-central", "Harlem")
            .when(col("NEIGHBORHOOD") === "Harlem-east", "East Harlem")
            .when(col("NEIGHBORHOOD") === "Harlem-upper", "Harlem")
            .when(col("NEIGHBORHOOD") === "Harlem-west", "Harlem")
            .when(col("NEIGHBORHOOD") === "Midtown Cbd", "Midtown")
            .when(col("NEIGHBORHOOD") === "Midtown East", "Midtown")
            .when(col("NEIGHBORHOOD") === "Midtown West", "Midtown")
            .when(col("NEIGHBORHOOD") === "Soho", "SoHo")
            .when(col("NEIGHBORHOOD") === "Upper East Side (59-79)", "Upper East Side")
            .when(col("NEIGHBORHOOD") === "Upper East Side (79-96)", "Upper East Side")
            .when(col("NEIGHBORHOOD") === "Upper East Side (96-110)", "Upper East Side")
            .when(col("NEIGHBORHOOD") === "Upper West Side (59-79)", "Upper West Side")
            .when(col("NEIGHBORHOOD") === "Upper West Side (79-96)", "Upper West Side")
            .when(col("NEIGHBORHOOD") === "Upper West Side (96-116)", "Upper West Side")
            .when(col("NEIGHBORHOOD") === "Washington Heights Lower", "Washington Heights")
            .when(col("NEIGHBORHOOD") === "Washington Heights Upper", "Washington Heights")
            .when(col("NEIGHBORHOOD") === "Airport La Guardia", "LaGuardia Airport")
            .when(col("NEIGHBORHOOD") === "Flushing Meadow Park", "Flushing Meadows Corona Park")
            .when(col("NEIGHBORHOOD") === "Flushing-north", "Flushing")
            .when(col("NEIGHBORHOOD") === "Flushing-south", "Flushing")
            .when(col("NEIGHBORHOOD") === "South Jamaica", "Jamaica")
            .when(col("NEIGHBORHOOD") === "So. Jamaica-baisley Park", "Jamaica")
            .when(col("NEIGHBORHOOD") === "Arrochar-shore Acres", "Shore Acres")
            .when(col("NEIGHBORHOOD") === "Bulls Head", "Bull's Head")
            .when(col("NEIGHBORHOOD") === "Fresh Kills", "Freshkills Park")
            .when(col("NEIGHBORHOOD") === "Great Kills-bay Terrace", "Bay Terrace")
            .when(col("NEIGHBORHOOD") === "West New Brighton", "West Brighton")
            .when(col("NEIGHBORHOOD") === "New Brighton-st. George", "St. George")
            .when(col("NEIGHBORHOOD") === "New Dorp-beach", "New Dorp Beach")
            .when(col("NEIGHBORHOOD") === "Princes Bay", "Prince's Bay")
            .when(col("NEIGHBORHOOD") === "Richmondtown-lighths Hill", "Lighthouse Hill")
            .when(col("NEIGHBORHOOD") === "Rossville-charleston", "Charleston")
            .when(col("NEIGHBORHOOD") === "Stapleton-clifton", "Clifton")
            .otherwise(col("NEIGHBORHOOD")) )   

    
    val formatPropertyType = nhNamesUpdated.withColumn("BUILDING_CLASS_CATEGORY", 
            when(col("BUILDING_CLASS_CATEGORY") like "%01 ONE FAMILY DWELLINGS%", "One_Family"). 
            when(col("BUILDING_CLASS_CATEGORY") like "%02 TWO FAMILY DWELLINGS%", "Two_Family").
            when(col("BUILDING_CLASS_CATEGORY") like "%03 THREE FAMILY DWELLINGS%", "Three_Family").
            when(col("BUILDING_CLASS_CATEGORY") like "%10 COOPS - ELEVATOR APARTMENTS%", "Coop_Elevator").
            when(col("BUILDING_CLASS_CATEGORY") like "%09 COOPS - WALKUP APARTMENTS%", "Coop_Walkup").
            when(col("BUILDING_CLASS_CATEGORY") like "%13 CONDOS - ELEVATOR APARTMENTS%", "Condo_Elevator").
            when(col("BUILDING_CLASS_CATEGORY") like "%12 CONDOS - WALKUP APARTMENTS%", "Condo_Walkup").
            otherwise(col("BUILDING_CLASS_CATEGORY"))) 


    //filter results for matching and non matchng neighborhood names
     // filter outs the extra null columns that the union created
    val mismatch = formatPropertyType.filter(!($"NEIGHBORHOOD".isin(namesList:_*)) && !($"NEIGHBORHOOD" == null)  )
    //mismatch.write.option("header", "true").format("csv").save("/user/sc2936/housingNamingConvention/mismatchNew")
    val matching = formatPropertyType.filter(($"NEIGHBORHOOD".isin(namesList:_*)) && !($"NEIGHBORHOOD" == null))

    val groupedDF = matching.groupBy($"NEIGHBORHOOD", $"BOROUGH",$"BUILDING_CLASS_CATEGORY").
        agg(min("SALE_PRICE") as "MIN_SALE_PRICE_2017_2018", 
        max("SALE_PRICE") as "MAX_SALE_PRICE_2017_2018", 
        mean("SALE_PRICE") as "AVERAGE_SALE_PRICE_2017_2018", 
        count("SALE_PRICE") as "NUMBER_OF_SALES").
        orderBy($"BOROUGH", $"NEIGHBORHOOD", $"BUILDING_CLASS_CATEGORY")


    groupedDF.coalesce(1).write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/summary_2017_2018_5.2.2018")
    groupedDF.schema


    //test re-reading the data and that column names are the same
    val testRead = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/summary_2017_2018_5.2.2018")
    testRead.limit(5).show
    testRead.count == groupedDF.count



//extract 2017 data only , include less building category types for data merge 
  val yearDF = matching.withColumn("SALE_YEAR", split($"SALE_DATE", "/")(2)).drop($"SALE_DATE")

  val filtYearDF = yearDF.filter((
    ($"BUILDING_CLASS_CATEGORY" like "%One_Family%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%Two_Family") ||
    ($"BUILDING_CLASS_CATEGORY" like "%Three_Family%")) &&
    ($"SALE_YEAR" like "%2017%") )


  //group the data
  // calculate the min sale price for each group, the max sale price for each group as well as the total number of sales
  val calc2017DF = filtYearDF.groupBy($"NEIGHBORHOOD", $"BOROUGH", $"BUILDING_CLASS_CATEGORY", $"SALE_YEAR").
    agg(count("SALE_PRICE") as "NUMBER_OF_SALES", mean("SALE_PRICE") 
    as "AVERAGE_SALE_PRICE").orderBy($"BOROUGH", $"NEIGHBORHOOD", $"BUILDING_CLASS_CATEGORY")

//this format will make it easier to merge with the rest of the historical data
 val calc2017cols = calc2017DF.select(
    $"NEIGHBORHOOD", 
    $"BUILDING_CLASS_CATEGORY", 
    $"NUMBER_OF_SALES", 
    $"AVERAGE_SALE_PRICE", 
    $"SALE_YEAR", 
    $"BOROUGH")


calc2017cols.coalesce(1).write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/summary_2017_5.1.18")


//format the recent data  (2017-2018) for merge, this will be used for filtering
 val filtRecentDF = groupedDF.filter((
    ($"BUILDING_CLASS_CATEGORY" like "%One_Family%") ||
    ($"BUILDING_CLASS_CATEGORY" like "%Two_Family") ||
    ($"BUILDING_CLASS_CATEGORY" like "%Three_Family%")) )

 val addYear = filtRecentDF.withColumn("SALE_YEAR", lit("2017")).withColumnRenamed("AVERAGE_SALE_PRICE_2017_2018", "AVERAGE_SALE_PRICE")

 val calcRecentcols = addYear.select(
    $"NEIGHBORHOOD", 
    $"BUILDING_CLASS_CATEGORY", 
    $"NUMBER_OF_SALES", 
    $"AVERAGE_SALE_PRICE", 
    $"SALE_YEAR", 
    $"BOROUGH")

 calcRecentcols.coalesce(1).write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/new2017_5.1.18")









