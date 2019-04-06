import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Scanner;
import java.util.LinkedList;

public class ExecutionEngine {
	
	private Catalog catalog;
	private int resultNumber;
	
	public ExecutionEngine() {
		resultNumber = 0;
		catalog = new Catalog();
	}
	
	public void setCatalog(Catalog catalog) {
		this.catalog = catalog;
	}
	
	public String predicateScan(String tableName, ArrayList<int[]> data) throws IOException {
		// int[] has
		// index 0 = column
		// index 1 = comparing value
		// index 2 = operator
		// 0 is =, 1 is <, 2 is >
		
		// for each predicate, map the column to a PredicateChecker Object
		HashMap<Integer, PredicateChecker> columnMap = new HashMap<>(); // each column maps to a predicate checker
		for (int i = 0; i < data.size(); i++) {
			int column = data.get(i)[0];
			int compareValue = data.get(i)[1];
			int operator = data.get(i)[2];
			
			if (!columnMap.containsKey(column)) { // first time seeing a predicate on a column
				PredicateChecker pc = new PredicateChecker();
				pc.addPredicate(operator, compareValue);
				columnMap.put(column, pc);
			} else { // more than 1 predicate on a column
				columnMap.get(column).addPredicate(operator, compareValue);
			}
		}
		
		// Gets list of columns we have to check
		HashSet<Integer> checkColumns = new HashSet<>(columnMap.keySet());
		
		// Reading Data
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tableName + ".dat")));
		
		// Get number of columns
		int numOfCols = catalog.getColumns(tableName);
		
		// DataOutputStream to write the output
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName + "-short.dat")));
		
		try {
			while (true) {
				int[] row = new int[numOfCols];
				for (int i = 0; i < numOfCols; i++)  // add row to our int array
					row[i] = dis.readInt();
				
				// iterates through the check columns and checks the predicate
				Iterator<Integer> itr = checkColumns.iterator();
				boolean leaveLoop = false;
				while (itr.hasNext() && !leaveLoop) {
					int column = itr.next();
					if (!columnMap.get(column).checkPredicates(row[column]))
						leaveLoop = true;
				}
				if (leaveLoop == false) { // leaveLoop is false if we checked all the column predicates and they were all true
					for (int i : row) // writes the row
						dos.writeInt(i);
				}
			}
		} catch (EOFException e) { // Done reading the file
			
		}
		dis.close();
		
		return tableName + "-short.dat";
	}
	
	public String equiJoinBNLJ(String table1FileName, String table1ColName, String table2FileName, String table2ColName) throws IOException {
		String[] table1Arr = catalog.getColumnNames(table1FileName.substring(0,1)).split(",");
		String[] table2Arr = catalog.getColumnNames(table2FileName.substring(0,1)).split(",");
		
		// Join columns for each table
		int table1JoinCol = findIndex(table1Arr, table1ColName);
		int table2JoinCol = findIndex(table2Arr, table2ColName);
		
		// Reading Table1 Data
		DataInputStream dis1 = new DataInputStream(new BufferedInputStream(new FileInputStream(table1FileName)));
		
		// Total columns for each table
		int table1NumCols = table1Arr.length;
		int table2NumCols = table2Arr.length;
		
		// Buffer
		Queue<int[]> table1Buffer = new LinkedList<>();
		
		// DataOutputStream to write the output
		String tableName = "result" + resultNumber + ".dat";
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName)));
		resultNumber++;

		try {
			while (true) { // Reads dis1 InputStream
				int[] table1Row = new int[table1NumCols];
				for (int i = 0; i < table1NumCols; i++) 
					table1Row[i] = dis1.readInt();
				table1Buffer.add(table1Row);

				if (table1Buffer.size() < 1000) continue;
				
				// Reading Table2 Data
				DataInputStream dis2 = new DataInputStream(new BufferedInputStream(new FileInputStream(table2FileName)));
				try { // Reads dis2 InputStream
					while (true) {
						int[] table2Row = new int[table2NumCols];
						for (int i = 0; i < table2NumCols; i++) 
							table2Row[i] = dis2.readInt();
						
						// Iterates over table1Buffer
						Iterator<int[]> itr = table1Buffer.iterator();
						while (itr.hasNext()) {
							int[] table1TempRow = itr.next();

							if (table1TempRow[table1JoinCol] == table2Row[table2JoinCol]) {
								// Writes the combined row of the 2 tables
								for (int i : table1TempRow) {
									dos.writeInt(i);
								}
								for (int i : table2Row) {
									dos.writeInt(i);
								}
							}
						}
					}
					
				} catch (EOFException e) { // Done reading table2
					
				}
				// Clears buffer
				table1Buffer.clear();
			}
		} catch (EOFException e) { // Done reading table1
			// If we reach the end of the file, but still have rows in our buffer

			// Reading Table2 Data
			DataInputStream dis2 = new DataInputStream(new BufferedInputStream(new FileInputStream(table2FileName)));
			try { // Reads dis2 InputStream
				while (true) {
					int[] table2Row = new int[table2NumCols];
					for (int i = 0; i < table2NumCols; i++) 
						table2Row[i] = dis2.readInt();
					
					// Iterates over table1Buffer
					Iterator<int[]> itr = table1Buffer.iterator();
					while (itr.hasNext()) {
						int[] table1TempRow = itr.next();
						//System.out.println(table1TempRow[table1JoinCol] + " " + table2Row[table2JoinCol]);
						if (table1TempRow[table1JoinCol] == table2Row[table2JoinCol]) {
							// Writes the combined row of the 2 tables
							for (int i : table1TempRow) {
								dos.writeInt(i);
							}
							for (int i : table2Row) {
								dos.writeInt(i);
							}
						}
					}
				}
				
			} catch (EOFException eof) { // Done reading table2
				
			}
		}
		
		return tableName;
	}

	
	
	public int findIndex(String[] columnNames, String columnName) {
		int i = 0;
		while (i < columnNames.length) {
			if (columnNames[i].equals(columnName)) return i;
			i++;
		}
		return -1;
	}
	

    public void executeQuery(Queue<String> columnsQueue, ArrayList<String> fromColumns, ArrayList<String> whereColumns, ArrayList<String> andColumns) throws FileNotFoundException {
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
                
                String table1 = "result" + resultNumber + ".dat";
                String table2 = joinTable + ".dat";
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
                
                String table1 = "result" + resultNumber + ".dat";
                String table2 = joinTable + ".dat";
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
            
            String table1 = leftTable + ".dat";
            String table2 = rightTable + ".dat";
            resultNumber++;
            // Joins the disjoint table
            joinTables(table1, getColumnNumber(table1, tablesArr[0]), table2, getColumnNumber(table2, tablesArr[1]), resultNumber, false);
            
            // Join with the current result
            table1 = "result" + (resultNumber - 1) + ".dat";
            table2 = "result" + resultNumber + ".dat";
            resultNumber++;
            // Joins the disjoint table join with the current result
            joinTables(table1, getColumnNumber(table1, tablesArr[0]), table2, getColumnNumber(table2, tablesArr[1]), resultNumber, true);
        }
        
        // PRINTS OUT RESULTS
        printOutResults(columnsQueue, resultNumber);
    }

    public int andPredicate(ArrayList<String> andColumns, StringBuilder table, int resultNumber) throws FileNotFoundException {
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
		Scanner andTableScanner = new Scanner(new File(table.toString() + ".dat"));
		PrintStream ps = new PrintStream(new File("result" + resultNumber + ".dat"));
		
		// Writes the header
		ps.println(andTableScanner.nextLine());
		
		int numOfColumns = countColumns(table.toString() + ".dat");
		
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

    public void joinTables(String table1, int table1JoinCol, String table2, int table2JoinCol, int resultNumber, boolean noPredicate) throws FileNotFoundException {
		Scanner t1Scanner = new Scanner(new File(table1));
		Scanner t2Scanner = new Scanner(new File(table2));
		
		// File we're writing to
		PrintStream ps = new PrintStream(new File("result" + resultNumber + ".dat"));
		
		
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

    public boolean containsTable(String expression, char tableName) {
		String[] splitExpression = expression.split("=");
		return splitExpression[0].charAt(0) == tableName || splitExpression[1].charAt(0) == tableName;
    }
    
    public int countColumns(String table) throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(table));
		return scanner.nextLine().split(",").length;
	}
	
	public int getColumnNumber(String table, String columnName) throws FileNotFoundException {
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

    public boolean determinePredicate(int columnValue, int benchmark, int operator) {
		if (operator == 0) return columnValue == benchmark;
		if (operator == 1) return columnValue > benchmark;
		if (operator == 2) return columnValue < benchmark;
		return false;
	}

    public void printOutResults(Queue<String> columnsQueue, int resultNumber) throws FileNotFoundException {
		int columnsQueueSize = columnsQueue.size();
		
		// Find SUM and print out result
		int[] cols = new int[columnsQueueSize]; // Column we are summing
		int[] colsSum = new int[columnsQueueSize]; // Sum of the columns
		while (!columnsQueue.isEmpty()) {
			int index = 0;
			cols[index] = getColumnNumber("result" + resultNumber + ".dat", columnsQueue.remove());
			index++;
		}
		
		int numOfColumns = countColumns("result" + resultNumber + ".dat");
		
		Scanner sumScanner = new Scanner(new File("result" + resultNumber + ".dat"));
		
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
}