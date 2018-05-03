import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RebuildJSON {

	public static void main(String[] args) throws Exception {
		Map<String, Values> fromCSV = new HashMap<>();
		Map<String, Integer> uniqueNeighborhoods = new HashMap<>();
		BufferedReader br = new BufferedReader(
				new FileReader(new File("C:/Hari/BDAD/JSON/New.csv")));
		br.readLine();
		String line;
		while ((line = br.readLine()) != null) {
			String[] words = line.split(",");
			Values values = fromCSV.get(words[0]) != null ? fromCSV.get(words[0]) : new Values();
			values.rank = words[3];
			values.score = words[4];
			values.setPrice(words[1], words[2]);
			fromCSV.put(words[0], values);
		}
		br.close();
		System.out.println();

		String json = "";
		br = new BufferedReader(new FileReader(new File("C:/Hari/BDAD/JSON/nyc.geojson")));
		while ((line = br.readLine()) != null) {
			json += line;
		}
		br.close();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);
		JsonNode features = rootNode.get("features");
		String result = "{\"type\": \"FeatureCollection\",\"features\":[";
		for (JsonNode feature : features) {
			result += "{\"type\": \"Feature\",";
			Values values = fromCSV.get(feature.get("properties").get("neighborhood").asText());
			if(values == null) {
				uniqueNeighborhoods.put(feature.get("properties").get("neighborhood").asText(), 0);
				values = new Values();
			}
			result += values;
			result += "\"properties\":" + feature.get("properties") + ",";
			result += "\"geometry\":" + feature.get("geometry") + "},";
		}
		result = result.substring(0, result.length() - 1) + "]}";
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("C:/Hari/BDAD/JSON/result_4.json")));
		bw.write(result);
		bw.close();
		for(Map.Entry<String, Integer> entry : uniqueNeighborhoods.entrySet()) {
			System.out.println(entry.getKey());
		}
		System.out.println("!!!");
	}
}

class Values {

	private String coopsWalkup = "-1", oneFamily = "-1", coopsElevator = "-1", twoFamily = "-1", threeFamily = "-1", condosElevator = "-1", condosWalkup = "-1";
	public String rank = "-1", score = "-1";

	public void setPrice(String family, String price) {
		switch (family) {
		case "Coop_Walkup":
			coopsWalkup = price;
			break;
		case "One_Family":
			oneFamily = price;
			break;
		case "Coop_Elevator":
			coopsElevator = price;
			break;
		case "Two_Family":
			twoFamily = price;
			break;
		case "Three_Family":
			threeFamily = price;
			break;
		case "Condo_Elevator":
			condosElevator = price;
			break;
		case "Condo_Walkup":
			condosWalkup = price;
			break;
		}
	}
	
	@Override
	public String toString() {
		return "\"Coop_Walkup" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + coopsWalkup + "}," +
				"\"One_Family" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + oneFamily + "}," +
				"\"Coop_Elevator" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + coopsElevator + "}," + 
				"\"Two_Family" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + twoFamily + "}," + 
				"\"Three_Family" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + threeFamily + "}," + 
				"\"Condo_Elevator" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + condosElevator + "}," + 
				"\"Condo_Walkup" + "\":{\"Rank\":" + rank + ",\"Score\":" + score + ",\"Price\":" + condosWalkup + "},";
	}

}