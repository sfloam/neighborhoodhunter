
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

    // read folders, add column for file path and borough name per folder
    val dfBronxH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/bronx_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("BRONX"))
    val dfBKH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/brooklyn_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("BROOKLYN"))
    val dfMHH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/manhattan_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("MANHATTAN"))
    val dfQueensH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/queens_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("QUEENS"))
    val dfSIH = sqlContext.read.format("csv").option("header", "true").load("housingSalesRaw/staten_island_sales_prices/*").withColumn("Path_Name", input_file_name()).withColumn("BOROUGH", lit("STATEN ISLAND"))

    //merged data from all files
    val mergedH = dfMHH.unionAll(dfBronxH).unionAll(dfQueensH).unionAll(dfBKH).unionAll(dfSIH)


    //extract a column for year and format column names
    val dfFile = mergedH.withColumn("File", split($"Path_Name", "/")(7)).drop($"Path_Name")
    val dfYear = dfFile.withColumn("SALE_YEAR", split($"File", " ")(0)).drop($"File")
    val dfColsRenameH = dfYear.withColumnRenamed("TYPE OF HOME", "BUILDING_CLASS_CATEGORY").
        withColumnRenamed("NUMBER OF SALES", "NUMBER_OF_SALES").
        withColumnRenamed("LOWEST SALE PRICE", "LOWEST_SALE_PRICE").
        withColumnRenamed("AVERAGE SALE PRICE", "AVERAGE_SALE_PRICE").
        withColumnRenamed("MEDIAN SALE PRICE", "MEDIAN_SALE_PRICE").
        withColumnRenamed("HIGHEST SALE PRICE", "HIGHEST_SALE_PRICE")


    //convert all price string values to double
    val dfColsSelectH = dfColsRenameH.selectExpr(
        "NEIGHBORHOOD", 
        "BUILDING_CLASS_CATEGORY", 
        "NUMBER_OF_SALES", 
        "translate(AVERAGE_SALE_PRICE, '$|,|-', '') AVERAGE_SALE_PRICE",
        "SALE_YEAR",
        "BOROUGH")



     val dfDoubleH = dfColsSelectH.selectExpr(
        "NEIGHBORHOOD", 
        "BOROUGH",
        "SALE_YEAR",
        "BUILDING_CLASS_CATEGORY", 
        "cast(NUMBER_OF_SALES as Int) NUMBER_OF_SALES",
        "cast(AVERAGE_SALE_PRICE as Double) AVERAGE_SALE_PRICE")


    //rename building class category names that have extra white spaces
    val dfFormatH = dfDoubleH.withColumn("NEIGHBORHOOD", trim($"NEIGHBORHOOD")).
        withColumn("BUILDING_CLASS_CATEGORY", trim($"BUILDING_CLASS_CATEGORY")).
        withColumn("BUILDING_CLASS_CATEGORY", when(col("BUILDING_CLASS_CATEGORY") === "01  ONE FAMILY HOMES", "01 ONE FAMILY HOMES").
            when(col("BUILDING_CLASS_CATEGORY") === "02  TWO FAMILY HOMES", "02 TWO FAMILY HOMES").
            when(col("BUILDING_CLASS_CATEGORY") === "03  THREE FAMILY HOMES", "03 THREE FAMILY HOMES").
            otherwise(col("BUILDING_CLASS_CATEGORY")) ) 


    //pivot data frame such that each year is it's own column, group by neighborhood
    val groupedDFH = dfFormatH.groupBy($"NEIGHBORHOOD", $"BOROUGH",$"BUILDING_CLASS_CATEGORY").pivot("SALE_YEAR").min("AVERAGE_SALE_PRICE").
        orderBy($"BOROUGH", $"NEIGHBORHOOD", $"BUILDING_CLASS_CATEGORY")




    
  