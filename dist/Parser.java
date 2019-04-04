import java.util.Scanner;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;

public class Parser {
	
	public void readQuery(Scanner queryScanner, Queue<String> columnsQueue, ArrayList<String> fromColumns, ArrayList<String> whereColumns, ArrayList<String> andColumns) {
		// Get SELECT line
		// Figure out which column from which table we are summing (looking for)
		
		// Saves the table and column name (in the right order)
		queryScanner.next();
		String selectLine = queryScanner.nextLine().replaceAll(" ", "");
		//System.out.println("selectLine is " + selectLine);
		String[] selectColumns = selectLine.split(",");
		for (int i = 0; i < selectColumns.length; i++) {
			String col = selectColumns[i].trim();
			columnsQueue.add(col.substring(4, 8)); 
		}
		//System.out.println(columnsQueue);
		
		// Get FROM line
		queryScanner.next();
		String fromLine = queryScanner.nextLine().replaceAll(" ", "");
		//System.out.println("fromLine is " + fromLine);
		//fromColumns = new ArrayList<String>(Arrays.asList(fromLine.split(",")));
		arrayToList(fromLine.split(","), fromColumns);
		
		// Get WHERE line
		queryScanner.next();
		String whereLine = queryScanner.nextLine().replaceAll(" ", "");
		//System.out.println("whereLine is " + whereLine);
		//whereColumns = new ArrayList<String>(Arrays.asList(whereLine.split("AND")));
		arrayToList(whereLine.split("AND"), whereColumns);
		
		
		// Get AND line
		queryScanner.next();
		String andLine = queryScanner.nextLine().replaceAll(" ", "");
		//System.out.println("andLine is " + andLine);
		//andColumns = new ArrayList<String>(Arrays.asList(andLine.split(";")));
		arrayToList(andLine.split(";"), andColumns);
		//System.out.println("andColumns is " + andColumns);
		
	}
	
	public static void arrayToList(String[] stringArr, ArrayList<String> stringList) {
		for (String s : stringArr) {
			stringList.add(s);
		}
	}
}