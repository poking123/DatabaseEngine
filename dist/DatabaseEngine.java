import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Queue;
import java.util.LinkedList;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class DatabaseEngine {
	public static void main(String[] args) throws FileNotFoundException {
		
		Loader loader = new Loader();
		
		// Get the CSV files
		//String CSVFiles = loader.getCSVFiles();
		String CSVFiles = "../data/xxxs\\B.csv,../data/xxxs\\C.csv,../data/xxxs\\A.csv,../data/xxxs\\D.csv,../data/xxxs\\E.csv";
		
		// Loader loads all the data into storage
		loader.readCSVFiles(CSVFiles);
		
		
		// Gets the SQL queries
		//Scanner queryScanner = new Scanner(System.in);
		Scanner queryScanner = new Scanner(new File("../data/xxxs\\queries.sql"));
		//int numOfQueries = queryScanner.nextInt();
		int numOfQueries = 1;
		
		// For each query
		Parser parser = new Parser();
		//for (int i = 0; i < numOfQueries; i++) {
		for (int i = 0; i < 1; i++) {
			int resultNumber = 0;
			// Gets the data from each line of 1 query
			Queue<String> columnsQueue = new LinkedList<>(); // ex: queue - 1:"D.c0", 2:"D.c4", 3:"C.c1"
			ArrayList<String> fromColumns = new ArrayList<>(); // ex: ["A", "B", "C", "D"]
			ArrayList<String> whereColumns = new ArrayList<>(); // ex: ["A.c1=B.c0", " A.c3=D.c0", "C.c2=D.c2"]
			ArrayList<String> andColumns = new ArrayList<>(); // ex: "[D.c3=-9496]"
			
			parser.readQuery(queryScanner, columnsQueue, fromColumns, whereColumns, andColumns);

			// blank line
			if (i != numOfQueries - 1) queryScanner.nextLine();
			
			// AND - Sigma
			StringBuilder andTable = new StringBuilder();
			resultNumber = andPredicate(andColumns, andTable, resultNumber);
			
			// WHERE
			String toRemove = "";
			for (String s : whereColumns) { // Do 1 join where andTable is used
				if (containsTable(s, andTable.toString().charAt(0))) {
					// parses the where column element
					String[] tablesArr = s.split("=");
					String[] leftTableArr = tablesArr[0].split(".c");
					String[] rightTableArr = tablesArr[1].split(".c");
					// Gets the tables and the desired columns
					String leftTable = leftTableArr[0];
					int leftTableCol = Integer.parseInt(leftTableArr[1]);
					String rightTable = rightTableArr[0];
					int rightTableCol = Integer.parseInt(rightTableArr[1]);
					// Gets the table and column that we are joining
					String joinTable =  "";
					int joinCol = -1;
					int table1JoinCol = -1;
					if (leftTable.equals(andTable.toString())) {
						joinTable =  rightTable;
						joinCol = rightTableCol;
						table1JoinCol = leftTableCol;
					} else {
						joinTable =  leftTable;
						joinCol = leftTableCol;
						table1JoinCol = rightTableCol;
					}
					
					String table1 = "result" + resultNumber + ".txt";
					String table2 = joinTable + ".txt";
					resultNumber++;
					
					joinTables(table1, table1JoinCol, table2, joinCol, resultNumber, false);
					toRemove = s;
					break;
				}
			}
			// Removes the join just executed
			whereColumns.remove(toRemove);
			
			// Iterates over the joins and does the ones that the AND predicate was on
			Iterator<String> itr = whereColumns.iterator();
			while (itr.hasNext()) {
				String join = itr.next();
				
				if (containsTable(join, andTable.toString().charAt(0))) { // andTable is just the table letter from the AND query
					// parses the where column element
					String[] tablesArr = join.split("=");
					String[] leftTableArr = tablesArr[0].split("\\.");
					String[] rightTableArr = tablesArr[1].split("\\.");
					// Gets the tables and the desired columns
					String leftTable = leftTableArr[0];
					int leftTableCol = Integer.parseInt(leftTableArr[1].substring(1));
					String rightTable = rightTableArr[0];
					int rightTableCol = Integer.parseInt(rightTableArr[1].substring(1));
					// Gets the table and column that we are joining
					String joinTable =  "";
					int joinCol = -1;
					boolean left = false;
					if (leftTable.equals(andTable.toString())) {
						joinTable =  rightTable;
						joinCol = rightTableCol;
						left = true;
					} else {
						joinTable =  leftTable;
						joinCol = leftTableCol;
					}
					
					String table1 = "result" + resultNumber + ".txt";
					String table2 = joinTable + ".txt";
					resultNumber++;
					String colName = left ? tablesArr[0] : tablesArr[1];
					joinTables(table1, getColumnNumber(table1, colName), table2, joinCol, resultNumber, false);
					itr.remove();
				}
			}
			
			// STILL NEED TO JOIN OTHER TABLES THAT AREN'T LIKE THE ANDTABLE COLUMN
			itr = whereColumns.iterator();
			while (itr.hasNext()) {
				String join = itr.next();
				
				// parses the where column element
				String[] tablesArr = join.split("=");
				String[] leftTableArr = tablesArr[0].split("\\.");
				String[] rightTableArr = tablesArr[1].split("\\.");
				// Gets the tables and the desired columns
				String leftTable = leftTableArr[0];
				int leftTableCol = Integer.parseInt(leftTableArr[1].substring(1));
				String rightTable = rightTableArr[0];
				int rightTableCol = Integer.parseInt(rightTableArr[1].substring(1));
				
				String table1 = leftTable + ".txt";
				String table2 = rightTable + ".txt";
				resultNumber++;
				// Joins the disjoint table
				joinTables(table1, getColumnNumber(table1, tablesArr[0]), table2, getColumnNumber(table2, tablesArr[1]), resultNumber, false);
				
				// Join with the current result
				table1 = "result" + (resultNumber - 1) + ".txt";
				table2 = "result" + resultNumber + ".txt";
				resultNumber++;
				// Joins the disjoint table join with the current result
				joinTables(table1, getColumnNumber(table1, tablesArr[0]), table2, getColumnNumber(table2, tablesArr[1]), resultNumber, true);
			}
			
			// PRINTS OUT RESULTS
			printOutResults(columnsQueue, resultNumber);
			
			
		}
		
		
		
		
		//int count = 0;
		//Runtime runtime = Runtime.getRuntime();
		//for (int i = 0; i < 10000000; i++) {
		//	count++;
		//	System.out.println(runtime.totalMemory() - runtime.freeMemory());
		//}
	}
	
	
	
	
	
	
	
	public static int andPredicate(ArrayList<String> andColumns, StringBuilder table, int resultNumber) throws FileNotFoundException {
		// gets the binary operator
		int operator = -1; // 0: =, 1: <, 2: >
		String[] operators = {"=", "<", ">"};
		int count = 0;
		
		String[] operatorArr = new String[1];
		while (operator == -1) {
			// determines the operator (0, 1, or 2)
			operatorArr = andColumns.get(0).split(operators[count]);
			if (operatorArr.length > 1) {
				operator = count;
			}
			count++;
		}
		
		// gets benchmark value (value to be compared)
		int benchmark = Integer.parseInt(operatorArr[1]);
		// splits the left side of the comparison
		String[] leftSide = operatorArr[0].split(".c");
		// Gets table
		table.append(leftSide[0]);
		// Gets column
		int column = Integer.parseInt(leftSide[1]);
		
		// Reads the AND table and filters by the predicate
		Scanner andTableScanner = new Scanner(new File(table.toString() + ".txt"));
		PrintStream ps = new PrintStream(new File("result" + resultNumber + ".txt"));
		
		// Writes the header
		ps.println(andTableScanner.nextLine());
		
		int numOfColumns = countColumns(table.toString() + ".txt");
		
		// goes through each row and returns the ones whose predicate is true
		while (andTableScanner.hasNext()) { 
			//String row = andTableScanner.nextLine();
			//String[] rowArray = row.split(" ");
			int[] tempRow = new int[numOfColumns];
			for (int i = 0; i < numOfColumns; i++) {
				tempRow[i] = andTableScanner.nextInt();
			}
			
			//int columnValue = Integer.parseInt(rowArray[column]);
			int columnValue = tempRow[column];
			
			// write row if predicate is true
			if (determinePredicate(columnValue, benchmark, operator)) {
				//ps.println(row);
				for (int i = 0; i < numOfColumns - 1; i++) {
					ps.print(tempRow[i] + " ");
				}
				ps.println(tempRow[numOfColumns - 1]);
			}
		}
		return resultNumber;
	}
	
	public static void joinTables(String table1, int table1JoinCol, String table2, int table2JoinCol, int resultNumber, boolean noPredicate) throws FileNotFoundException {
		Scanner t1Scanner = new Scanner(new File(table1));
		Scanner t2Scanner = new Scanner(new File(table2));
		
		// File we're writing to
		PrintStream ps = new PrintStream(new File("result" + resultNumber + ".txt"));
		
		
		String header1 = t1Scanner.nextLine();
		String header2 = t2Scanner.nextLine();
		
		int table1Cols = header1.split(",").length;
		int table2Cols = header2.split(",").length;
		
		// Writes header of new files
		ps.print(header1 + ",");
		ps.println(header2);
		
		while (t1Scanner.hasNext()) {
			// Reads 1 row of table 1
			int i = 0;
			int[] t1TempRow = new int[table1Cols];
			while (i < table1Cols) {
				t1TempRow[i] = t1Scanner.nextInt();
				i++;
			}
			// READS ALL ROWS OF TABLE 2
			t2Scanner = new Scanner(new File(table2));
			// Assuming the first row is the header columns
			t2Scanner.nextLine();
			
			while (t2Scanner.hasNext()) {
				int j = 0;
				int[] t2TempRow = new int[table2Cols];
				while (j < table2Cols) {
					t2TempRow[j] = t2Scanner.nextInt();
					j++;
				}
				
				// Checks Predicate
				if (noPredicate || t1TempRow[table1JoinCol] == t2TempRow[table2JoinCol]) { // if true
					// Write to new File
					for (int k = 0; k < t1TempRow.length; k++) {
						ps.print(t1TempRow[k] + " ");
					}
					
					for (int k = 0; k < t2TempRow.length - 1; k++) {
						ps.print(t2TempRow[k] + " ");
					}
					ps.println(t2TempRow[t2TempRow.length - 1]); // prints the last value with no space
				}
			}
		}
		
		
	}
	
	public static void printOutResults(Queue<String> columnsQueue, int resultNumber) throws FileNotFoundException {
		int columnsQueueSize = columnsQueue.size();
		
		// Find SUM and print out result
		int[] cols = new int[columnsQueueSize]; // Column we are summing
		int[] colsSum = new int[columnsQueueSize]; // Sum of the columns
		while (!columnsQueue.isEmpty()) {
			int index = 0;
			cols[index] = getColumnNumber("result" + resultNumber + ".txt", columnsQueue.remove());
			index++;
		}
		
		int numOfColumns = countColumns("result" + resultNumber + ".txt");
		
		Scanner sumScanner = new Scanner(new File("result" + resultNumber + ".txt"));
		
		// Skips Header
		sumScanner.nextLine();
		
		// goes through each row and sums the rows
		while (sumScanner.hasNext()) {
			int[] tempRow = new int[numOfColumns];
			for (int j = 0; j < numOfColumns; j++) {
				tempRow[j] = sumScanner.nextInt();
			}
			
			for (int j = 0; j < colsSum.length; j++) {
				colsSum[j] += tempRow[cols[j]];
			}
		}
		
		// Prints out the output
		for (int j = 0; j < colsSum.length - 1; j++) {
			System.out.print(colsSum[j] + ",");
		}
		System.out.println(colsSum[colsSum.length - 1]);
	}
	
	public static int countColumns(String table) throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(table));
		return scanner.nextLine().split(",").length;
	}
	
	public static int getColumnNumber(String table, String columnName) throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(table));
		String[] columns = scanner.nextLine().split(",");
		int i = 0;
		while (i < columns.length) {
			if (columns[i].equals(columnName)) {
				return i;
			}
			i++;
		}
		System.out.println("Shouldn't have got here. Column not found in table. getColumnNumber");
		return -1;
	}
	
	public static boolean determinePredicate(int columnValue, int benchmark, int operator) {
		if (operator == 0) return columnValue == benchmark;
		if (operator == 1) return columnValue > benchmark;
		if (operator == 2) return columnValue < benchmark;
		return false;
	}
	
	public static boolean containsTable(String expression, char tableName) {
		String[] splitExpression = expression.split("=");
		return splitExpression[0].charAt(0) == tableName || splitExpression[1].charAt(0) == tableName;
	}
	
	
}

	