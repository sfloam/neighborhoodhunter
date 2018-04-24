import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NeighborhoodMatch {
	public static void main(String[] args) throws IOException {

		String currentLine;
		int expectedCount = 0;

		Map<String, String> expected = new HashMap<>();
		Map<String, String> actual = new HashMap<>();

		BufferedReader bufferedReader1 = new BufferedReader(new FileReader(new File("NHoodNameCentroids.csv")));
		BufferedReader bufferedReader2 = new BufferedReader(new FileReader(new File("ExpectedNeighborhoods.csv")));
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("Result.csv")));
		bufferedWriter.write(bufferedReader1.readLine());

		while ((currentLine = bufferedReader2.readLine()) != null) {
			expected.put(currentLine.trim(), currentLine.trim());
		}
		while ((currentLine = bufferedReader1.readLine()) != null) {
			String actualNeighborhood = currentLine.split(",")[0];
			actual.put(actualNeighborhood, currentLine);
		}

		bufferedReader1.close();
		bufferedReader2.close();

		for (String actualNeighbor : actual.keySet()) {
			if (expected.get(actualNeighbor) != null) {
				bufferedWriter.write("\n" + actual.get(actualNeighbor));
				expected.remove(actualNeighbor);
			} else {
				Iterator it = expected.keySet().iterator();
				while (it.hasNext()) {
					String expectedNeighbor = (String) it.next();
					if (expectedNeighbor.toLowerCase().equals(actualNeighbor.toLowerCase())) {
						String line = actual.get(actualNeighbor);
						String[] words = line.split(",");
						String output = expectedNeighbor + "," + words[1] + "," + words[2] + "," + words[3] + ","
								+ words[4] + "," + words[5] + "," + words[6];
						bufferedWriter.write("\n" + output);
						System.out.println(output);
						it.remove();
					}
				}
			}
		}
		for (String expectedNeighbor : expected.keySet()) {
			expectedCount++;
			bufferedWriter.write("\n" + expectedNeighbor + "," + expectedCount);
		}
		bufferedWriter.close();
	}
}
