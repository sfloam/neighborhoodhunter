import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class PrecinctCorrelation {

	private static final String REQUEST_METHOD = "GET";
	private static final String URL_PREFIX = "https://maps.googleapis.com/maps/api/place/textsearch/xml?key=";
	private static final String URL_SUFFIX = "&query=precinct&location=";
	private static final String API_KEY = "AIzaSyAzjNvgdoZFiKz6_Ej7DTQFXag6TnwfiFk";
	private static final String PRECINCT_1 = "pct.";
	private static final String PRECINCT_2 = "precinct";

	public static void main(String[] args) throws Exception {

		BufferedReader neighborhood = new BufferedReader(
				new FileReader(new File("C:\\Hari\\BDAD\\Data\\NHoodNameCentroids.csv")));
		int row;
		String line = neighborhood.readLine(); // read header
		while ((line = neighborhood.readLine()) != null) {
			row = Integer.parseInt(line.split(",")[0]);
			String[] coordinates = line.split("\\(")[1].split("\\)")[0].split(" ");
			String lat_lon = coordinates[1] + ",%20" + coordinates[0];
			URL url = new URL(URL_PREFIX + API_KEY + URL_SUFFIX + lat_lon);
			// System.out.println(url);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(REQUEST_METHOD);
			connection.connect();

			BufferedReader stream = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String response;
			while ((response = stream.readLine()) != null) {
				response = response.toLowerCase();
				if (response.contains(PRECINCT_1)) {
					extractPrecinct(response, PRECINCT_1, row);
					break;
				}
				if (response.contains(PRECINCT_2)) {
					extractPrecinct(response, PRECINCT_2, row);
					break;
				}
			}
			stream.close();
		}
		neighborhood.close();
	}

	private static void extractPrecinct(String response, String regularExpression, int row) {
		System.out.print("\n" + row + " ");
		int index = response.indexOf(regularExpression);
		for (int i = 10; i > 0; i--) {
			if (Character.isDigit(response.charAt(index - i))) {
				System.out.print(response.charAt(index - i));
			}
		}
	}

}
