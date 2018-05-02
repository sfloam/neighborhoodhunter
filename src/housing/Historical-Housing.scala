//author: sc2936@nyu.edu
//This program creates summaries for NYC housing sales from 2005 - 2017 
//The output only contains data from 2015 - 2017 for consistency across all 3 data sets
//The output of this program must be used as input to model

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

    // read folders, add column for file path and borough name per folder
    val dfBronxH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/bronx_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("BRONX"))
    val dfBKH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/brooklyn_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("BROOKLYN"))
    val dfMHH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/manhattan_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("MANHATTAN"))
    val dfQueensH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/queens_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("QUEENS"))
    val dfSIH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/staten_island_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("STATEN ISLAND"))
    val mergedH = dfMHH.unionAll(dfBronxH).unionAll(dfQueensH).unionAll(dfBKH).unionAll(dfSIH)

 
    val df2017 = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/new2017_5.1.18")
   
        
    //extract a column for year and format column names
    val dfFile = mergedH.withColumn("File", split($"Path_Name", "/")(7)).drop($"Path_Name")
    val dfYear = dfFile.withColumn("SALE_YEAR", split($"File", " ")(0)).drop($"File")
    val dfColsRenameH = dfYear.withColumnRenamed("TYPE OF HOME", "BUILDING_CLASS_CATEGORY").
        withColumnRenamed("NUMBER OF SALES", "NUMBER_OF_SALES").
        withColumnRenamed("LOWEST SALE PRICE", "LOWEST_SALE_PRICE").
        withColumnRenamed("AVERAGE SALE PRICE", "AVERAGE_SALE_PRICE").
        withColumnRenamed("MEDIAN SALE PRICE", "MEDIAN_SALE_PRICE").
        withColumnRenamed("HIGHEST SALE PRICE", "HIGHEST_SALE_PRICE")


   //remove symbols which will prevent converting string to Double
    val dfColsSelectH = dfColsRenameH.selectExpr(
        "NEIGHBORHOOD", 
        "BUILDING_CLASS_CATEGORY", 
        "NUMBER_OF_SALES", 
        "translate(AVERAGE_SALE_PRICE, '$|,|-', '') AVERAGE_SALE_PRICE",
        "SALE_YEAR",
        "BOROUGH")


    //merge with 2017 data
    val completeDataH = dfColsSelectH.unionAll(df2017)

     val dfDoubleH = completeDataH.selectExpr(
        "NEIGHBORHOOD", 
        "BUILDING_CLASS_CATEGORY",
        "BOROUGH",
        "SALE_YEAR",
        "cast(NUMBER_OF_SALES as Int) NUMBER_OF_SALES",
        "cast(AVERAGE_SALE_PRICE as Double) AVERAGE_SALE_PRICE")


    //rename building class category names that have extra white spaces and to match d3 schema
    val dfFormatH = dfDoubleH.withColumn("NEIGHBORHOOD", trim($"NEIGHBORHOOD")).
        withColumn("BUILDING_CLASS_CATEGORY", trim($"BUILDING_CLASS_CATEGORY")).
        withColumn("BUILDING_CLASS_CATEGORY", 
            when(col("BUILDING_CLASS_CATEGORY") === "01  ONE FAMILY HOMES", "One_Family").
            when(col("BUILDING_CLASS_CATEGORY") === "01 ONE FAMILY HOMES", "One_Family").
            when(col("BUILDING_CLASS_CATEGORY") === "02  TWO FAMILY HOMES", "Two_Family").
            when(col("BUILDING_CLASS_CATEGORY") === "02 TWO FAMILY HOMES", "Two_Family").
            when(col("BUILDING_CLASS_CATEGORY") === "03  THREE FAMILY HOMES", "Three_Family").
            when(col("BUILDING_CLASS_CATEGORY") === "03 THREE FAMILY HOMES", "Three_Family").
            otherwise(col("BUILDING_CLASS_CATEGORY")) )


    //neighborhood name matching
    val names = sqlContext.read.format("csv").option("header", "true").load("housingNameConvention/LookupTable.csv")
    val namesList = names.select("Name").rdd.map(r => r(0)).collect.toList

    val nhNameSPlit1H = dfFormatH.withColumn("NEIGHBORHOOD", split($"NEIGHBORHOOD", "/")(0))
    val nhNameSPlit12H = dfFormatH.withColumn("NEIGHBORHOOD", split($"NEIGHBORHOOD", "/")(1)) 
    val mergedNHnamesH = nhNameSPlit1H.unionAll(nhNameSPlit12H)
    val nhFormatLowerH = mergedNHnamesH.withColumn("NEIGHBORHOOD", lower(col("NEIGHBORHOOD")))
    val nhFormatCamelH = nhFormatLowerH.select(initcap($"NEIGHBORHOOD") as "NEIGHBORHOOD", 
        $"BOROUGH", $"SALE_YEAR", $"BUILDING_CLASS_CATEGORY", $"NUMBER_OF_SALES", $"AVERAGE_SALE_PRICE" )

    
     val nhNamesUpdatedH = nhFormatCamelH.withColumn("NEIGHBORHOOD", 
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

    val matchingH = nhNamesUpdatedH.filter(($"NEIGHBORHOOD".isin(namesList:_*)) && !($"NEIGHBORHOOD" == null))
    val mismatchH = nhNamesUpdatedH.filter(!($"NEIGHBORHOOD".isin(namesList:_*)) && !($"NEIGHBORHOOD" == null))

    val mismatchView = mismatchH.groupBy($"NEIGHBORHOOD", $"BOROUGH",$"BUILDING_CLASS_CATEGORY").pivot("SALE_YEAR").agg(mean("AVERAGE_SALE_PRICE") as "AVERAGE_SALE_PRICE").
        orderBy($"BOROUGH", $"NEIGHBORHOOD")

    //mismatchView.show

  
  //pivot data frame such that each year is it's own column ex ("2015", "2016", "2017") (versus all in a single column)
    val groupedDFH = matchingH.groupBy($"NEIGHBORHOOD", $"BOROUGH").pivot("SALE_YEAR").agg(mean("AVERAGE_SALE_PRICE") as "AVERAGE_SALE_PRICE").
        orderBy($"BOROUGH", $"NEIGHBORHOOD")


    //only includes historical years which the other data sets have as well
    val dropColsDFH = groupedDFH.select(
        $"NEIGHBORHOOD", 
        $"BOROUGH", 
        $"2015", 
        $"2016", 
        $"2017")


    //add every property TYPE
    val condoE = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("Condo_Elevator"))
    val condoW = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("Condo_Walkup"))
    val coopE = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("Coop_Elevator"))
    val coopW = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("Coop_Walkup"))
    val oneFamily = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("One_Family"))
    val twoFamily = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("Two_Family"))
    val threeFamily = dropColsDFH.withColumn("BUILDING_CLASS_CATEGORY", lit("Three_Family"))

    val allBuildingTypes = condoE.unionAll(condoW).unionAll(coopE).unionAll(coopW).unionAll(oneFamily).unionAll(twoFamily).unionAll(threeFamily)
  
    //find the max value for d3 input
    val maxBudget = dropColsDFH.groupBy($"BOROUGH", $"NEIGHBORHOOD").agg(max("2015") as "2015", max("2016") as "2016", max("2017") as "2017")

    //calculate variance data - variance looks at the percent change in a metric from historical data to 2017
    // all data sources follow this same calculation
    val varianceDF1 = allBuildingTypes.withColumn("Avg", ($"2015" + $"2016") / 2)
    val varianceDF2 = varianceDF1.withColumn("Diff", $"2017" - $"Avg")
    val varianceDFTotal = varianceDF2.withColumn("Variance", $"Diff" / $"2017")
    val variancePrediction = varianceDFTotal.withColumn("Variance_prediction", ($"2016" - $"2015") / $"2016")

    //housing data only creates a metric to test the variance prediction against.
    //we choose percent increase in housing price, as ultimately we are targeting good investments
    val varianceTest = variancePrediction.withColumn("Variance_Test", ($"2017" - $"2016") / $"2017")
    //val varianceSorted = varianceDFTotal.sort(asc("Variance"))

    // replace any null values in output with 0.
    val map = Map("Variance" -> 0.0, "Variance_prediction" -> 0.0, "Variance_Test" -> 0.0)
    val replaceNull = varianceTest.na.fill(map)

   replaceNull.coalesce(1).write.option("header", "true").format("csv").save("/user/sc2936/housingSalesClean/historical_all_buildingType_5.1.18")
   //val test = sqlContext.read.format("csv").option("header", "true").load("housingSalesClean/historical_all_buildingType_5.1.18")

