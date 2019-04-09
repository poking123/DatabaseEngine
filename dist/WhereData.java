
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

public class WhereData {

	private ArrayList<HashMap<Character, String>> tables;
	
	public WhereData() {
		
		tables = new ArrayList<>();
	}
	
	public ArrayList<HashMap<Character, String>> getTables() {
		// tables is just an arrayList with maps of equijoin tables
		return this.tables;
	}

	public void addWhereData(String whereLine) {
		Scanner tempScanner = new Scanner(whereLine);
		while (tempScanner.hasNext()) {
			HashMap<Character, String> tempMap = new HashMap<>();
			
			tempScanner.next(); // WHERE or AND
			
			String leftTableAndColumn = tempScanner.next();
			char leftTableChar = leftTableAndColumn.charAt(0);
			
			int cIndex = leftTableAndColumn.indexOf('c');
			//String leftColumn = leftTableAndColumn.substring(cIndex, leftTableAndColumn.length());
			
			tempMap.put(leftTableChar, leftTableAndColumn); // add to map
			
			tempScanner.next(); // Equal Operator (equi-join)
			String rightTableAndColumn = tempScanner.next();
			char rightTableChar = rightTableAndColumn.charAt(0);
			
			cIndex = rightTableAndColumn.indexOf('c');
			//String rightColumn = rightTableAndColumn.substring(cIndex, rightTableAndColumn.length());
			
			tempMap.put(rightTableChar, rightTableAndColumn); // add to map
			
			tables.add(tempMap);
		}
		tempScanner.close();
	}
	
}
