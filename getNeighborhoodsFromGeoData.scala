val geodataRDD = sc.textFile("hdfs:///user/smf463/bdad-project/geodata/nyc.geojson")
val neighborhoods = geodataRDD.flatMap(line=>line.split(",")).    
filter(_.contains("neighborhood")).    
map(line=>line.
    replace("\"","")
    replace("properties: {","")
    trim()
    replace("+ "," ")
    trim()
    replace("neighborhood:","")
    trim())



/*
hdfs dfs -get geodata/neighborhoodsFromGeo/part* geodata/

//CLI
touch neighborhoods.txt
cat part-00001 >> part-00000
awk '!seen[$0]++' part-00000 > neighborhoods.txt
*/