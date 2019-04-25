
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

public class AndData {

	private HashMap<Character, ArrayList<int[]>> tablePredicateMap;
	
	public AndData() {		
		tablePredicateMap = new HashMap<>();
	}
	
	public HashSet<Character> getTableNames() {
		return new HashSet<>(tablePredicateMap.keySet());
	}
	
	// Character to list of int[]
	// inside each int[] is
	// index 0 - column
	// index 1 - operator
	// index 2 - compare value
	// index 3 - original column
	public HashMap<Character, ArrayList<int[]>> getTablePredicateMap() {
		return this.tablePredicateMap;
	}

	
	public void addAndData(String andLine) {
		Scanner tempScanner = new Scanner(andLine);
		while (tempScanner.hasNext()) {
			tempScanner.next(); // AND
			
			String tableAndColumn = tempScanner.next();
			char tableNameChar = tableAndColumn.charAt(0);
			
			int columnNumber = Integer.parseInt(tableAndColumn.substring(3, tableAndColumn.length()));
			
			String operator = tempScanner.next();
			int operatorNum = -1;
			// add operator
			if (operator.equals("=")) {
				operatorNum = 0;
			} else if (operator.equals("<")) {
				operatorNum = 1;
			} else {
				operatorNum = 2;
			}
			
			int compareValue = tempScanner.nextInt();
			
			// index 0 - column
			// index 1 - operator
			// index 2 - compare value
			int[] data = {columnNumber, operatorNum, compareValue, columnNumber};
			ArrayList<int[]> dataList = new ArrayList<>();
			// One table is mapped to a list of predicates
			if (tablePredicateMap.containsKey(tableNameChar)) {
				dataList = tablePredicateMap.get(tableNameChar);
			}
			dataList.add(data);
			tablePredicateMap.put(tableNameChar, dataList);
		}
		tempScanner.close();
	}
	
	
}
