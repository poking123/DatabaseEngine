import java.util.Scanner;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;

public class Parser {
	
	private int resultNumber;
	private Queue<String> columnsQueue; // ex: queue - 1:"D.c0", 2:"D.c4", 3:"C.c1"
	private ArrayList<String> fromColumns; // ex: ["A", "B", "C", "D"]
	private ArrayList<String> whereColumns; // ex: ["A.c1=B.c0", " A.c3=D.c0", "C.c2=D.c2"]
	private ArrayList<String> andColumns; // ex: "[D.c3=-9496]"

	public Parser() {
		resultNumber = 0;
	}

	public Queue<String> getColumnsQueue() {
		return this.columnsQueue;
	}

	public ArrayList<String> getFromColumns() {
		return this.fromColumns;
	}

	public ArrayList<String> getWhereColumns() {
		return this.whereColumns;
	}

	public ArrayList<String> getAndColumns() {
		return this.andColumns;
	}

	public void readQuery(Scanner queryScanner) {

		columnsQueue = new LinkedList<>(); // ex: queue - 1:"D.c0", 2:"D.c4", 3:"C.c1"
		fromColumns = new ArrayList<>(); // ex: ["A", "B", "C", "D"]
		whereColumns = new ArrayList<>(); // ex: ["A.c1=B.c0", " A.c3=D.c0", "C.c2=D.c2"]
		andColumns = new ArrayList<>(); // ex: "[D.c3=-9496]"

		// Get SELECT line
		// Figure out which column from which table we are summing (looking for)
		// Saves the table and column name (in the right order)
		queryScanner.next();
		String selectLine = queryScanner.nextLine().replaceAll(" ", "");
		String[] selectColumns = selectLine.split(",");
		for (int i = 0; i < selectColumns.length; i++) {
			String col = selectColumns[i].trim();
			columnsQueue.add(col.substring(4, 8)); 
		}
		
		// Get FROM line
		queryScanner.next();
		String fromLine = queryScanner.nextLine().replaceAll(" ", "");
		arrayToList(fromLine.split(","), fromColumns);
		
		// Get WHERE line
		queryScanner.next();
		String whereLine = queryScanner.nextLine().replaceAll(" ", "");
		arrayToList(whereLine.split("AND"), whereColumns);
		
		
		// Get AND line
		queryScanner.next();
		String andLine = queryScanner.nextLine().replaceAll(" ", "");
		arrayToList(andLine.split(";"), andColumns);		
	}

	public int getNumOfQueries(Scanner queryScanner) {
		return 1;
		// return queryScanner.nextInt();
	}
	
	public void arrayToList(String[] stringArr, ArrayList<String> stringList) {
		for (String s : stringArr) {
			stringList.add(s);
		}
	}
}