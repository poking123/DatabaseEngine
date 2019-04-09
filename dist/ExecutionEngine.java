
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ExecutionEngine {
	
	private final int bufferSize = 1000;
	private int resultNumber;
	private Deque<String> finalDeque;

	int finalNum = 0;
	
	
	public ExecutionEngine() {
		resultNumber = 0;
		finalDeque = new ArrayDeque<>();
	}
	
	public void executeQuery(Deque<RAOperation> tableDeque, Queue<Predicate> predicateQueue) {
		Queue<RAOperation> resultQueue = new LinkedList<>();
		
		while (!predicateQueue.isEmpty()) {
			Predicate currentPredicate = predicateQueue.remove();
			switch (currentPredicate.getType()) {
				case "filterPredicate": // take off from deque, filter, and put on result queue
					RAOperation operation = tableDeque.pop();
					FilterPredicate fp = (FilterPredicate) currentPredicate;
					Filter filter = new Filter(operation, fp);
					resultQueue.add(filter); // adds result to result queue
					break;
					
				case "equijoinPredicate": 
					// makes sure result queue size is 2
					while (resultQueue.size() < 2) {
						resultQueue.add(tableDeque.pop());
					}
					
					EquijoinPredicate ep = (EquijoinPredicate) currentPredicate;
					Equijoin equijoin = new Equijoin(resultQueue.remove(), resultQueue.remove(), ep);
					resultQueue.add(equijoin); // adds result to result queue
					break;
			}
		}
	}
	
	public void executeQuery(String selectColumnNames, HashMap<Character, ArrayList<int[]>> tablePredicateMap, HashMap<Character, Queue<String[]>> predicateJoinQueueMap, Deque<String[]> disjointDeque) throws IOException {
		
		// Get set of predicates
		//HashSet<Character> predicateTables = new HashSet<>(tablePredicateMap.keySet());
		//Iterator<Character> predicateTablesItr = predicateTables.iterator();
		String result = "";
		// For each Predicate Table
//		while (predicateTablesItr.hasNext()) { 
//			char predicateTable = predicateTablesItr.next();
//			// Perform predicateScan
//			ArrayList<int[]> predicateScanData = tablePredicateMap.get(predicateTable);
//			result = predicateScan(Character.toString(predicateTable), predicateScanData);
//			
//			// Get Predicate Queue
//			Queue<String[]> predicateQueue = predicateJoinQueueMap.get(predicateTable);
//			// Equijoin other tables (in the queue)
//			while (!predicateQueue.isEmpty()) { 
//				// index 0 - predicateTableColumnName
//				// index 1 - otherTableName
//				// index 2 - otherTableColumnName
//				String[] tempTableData = predicateQueue.remove();
//				//System.out.println("Predicates equijoin:");
//				//System.out.println(result);
//				//System.out.println(tempTableData[1] + ".dat");
//				result = equijoinBNLJ(result, tempTableData[0], tempTableData[1] + ".dat", tempTableData[2]);
//			}
//			
//			// Put result in finalDeque
//			finalDeque.push(result);
//			System.out.println("in while loop");
//		}
		System.out.println("out of while loop");
		//System.out.println("disjointDeque:");
		// For each element in disjointDeque
		while (!disjointDeque.isEmpty()) {
			System.out.println("in disjointDeque loop");
			// index 0 - table1 name
			// index 1 - table1 column name
			// index 2 - table2 name
			// index 3 - table2 column name
			String[] equijoinData = disjointDeque.pop();
			//System.out.println("disjointDeque Equijoin:");
			result = equijoinBNLJ(equijoinData[0], equijoinData[1], equijoinData[2], equijoinData[3]);
			// Put result in finalDeque
			finalDeque.push(result);
		}
		System.out.println("out of disjointDeque loop");
		// Cross Product all tables in finalDeque
		while (finalDeque.size() != 1) {
			String table1 = finalDeque.pop();
			String table2 = finalDeque.pop();
			//System.out.println("finalDeque:");
			// NEED TO CHANGE TO EQUIJOIN OR NOPREDICATEJOIN based on join columns
			result = noPredicateJoin(table1, table2);
			finalDeque.push(result);
		}
		
		// Get the sum of the selected columns and print the results
		sumAggregate(result, selectColumnNames);
	}
	
	public String predicateScan(String tableName, ArrayList<int[]> data) throws IOException {
		// System.out.println("Predicate Scan: " + tableName);

		// int[] has
		// index 0 = column
		// index 1 = operator // 0 is =, 1 is <, 2 is >
		// index 2 = comparing value
		
		
		// for each predicate, map the column to a PredicateChecker Object
		HashMap<Integer, PredicateChecker> columnMap = new HashMap<>(); // each column maps to a predicate checker
		for (int i = 0; i < data.size(); i++) {
			int[] predicateData = data.get(i);
			int column = predicateData[0];
			int operator = predicateData[1];
			int compareValue = predicateData[2];
			
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
		int numOfCols = Catalog.getColumns(tableName);
		
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
					for (int i : row) { // writes the row
						dos.writeInt(i);
						// System.out.print(i + " ");
					}
					// System.out.println();
					// System.out.println();
				}
			}
		} catch (EOFException e) { // Done reading the file
			
		}
		dis.close();
		dos.close();
		
		TableMetaData metadata = new TableMetaData(Catalog.getHeader(tableName)); // header is the same for a predicate scan
		Catalog.addData(tableName + "-short", metadata);
		
		return tableName + "-short.dat";
	}
	
	public String equijoinBNLJ(String table1FileName, String table1ColName, String table2FileName, String table2ColName) throws IOException {
		//System.out.println("Equijoin:");
		int table1DotIndex = table1FileName.indexOf('.');
		int table2DotIndex = table2FileName.indexOf('.');

		String table1Name = table1FileName.substring(0, table1DotIndex);
		String table2Name = table2FileName.substring(0, table2DotIndex);
		// System.out.println("table1Name: " + table1Name + " - EE 164");
		// System.out.println("table1Name: " + table2Name + " - EE 165");
		String table1Header = Catalog.getHeader(table1Name);
		String table2Header = Catalog.getHeader(table2Name);
		// System.out.println(table1Header + " - EE 173");
		// System.out.println(table2Header + " - EE 174");

		String[] table1Arr = table1Header.split(",");
		String[] table2Arr = table2Header.split(",");

		// System.out.println(table1ColName + " - EE 179");
		// System.out.println(table2ColName + " - EE 180");
		
		// DataOutputStream to write the output
		String result = "result" + resultNumber;
		String tableName = result + ".dat";
		
		// Total columns for each table
		int table1NumCols = table1Arr.length;
		int table2NumCols = table2Arr.length;
		
		TableMetaData metadata = new TableMetaData("");
		
		// Checks if table2 column is already in table 1
		if (table1Header.indexOf(table2ColName) == -1) { // table 1 does not contain table2ColName
			
			// Join columns for each table
			int table1JoinCol = findIndex(table1Arr, table1ColName);
			int table2JoinCol = findIndex(table2Arr, table2ColName);
			
			equijoinTwoTables(tableName, table1FileName, table2FileName, table1NumCols, table2NumCols, table1JoinCol, table2JoinCol);
			metadata = new TableMetaData(table1Header + "," + table2Header);
			
		} else { // table 1 does contains table2ColName
			// Join columns for each table
			int table1JoinCol = findIndex(table1Arr, table1ColName);
			int table2JoinCol = findIndex(table1Arr, table2ColName);
			
			equijoinOneTable(tableName, table1FileName, table1NumCols, table1JoinCol, table2JoinCol);
			metadata = new TableMetaData(table1Header);
		}

		// Metadata for the new table
		Catalog.addData(result, metadata);
		if (table1Name.length() > 1) 
			Catalog.removeData(table1Name);
		if (table2Name.length() > 1)
		Catalog.removeData(table2Name);
		
		return tableName;
	}
	
	public void equijoinOneTable(String tableName, String table1FileName, int table1NumCols, int table1JoinCol, int table2JoinCol) throws IOException {
		// Reading Table1 Data
		DataInputStream dis1 = new DataInputStream(new BufferedInputStream(new FileInputStream(table1FileName)));
		
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName)));
		resultNumber++;
		
		try {
			while (true) {
				int[] table1Row = new int[table1NumCols];
				for (int i = 0; i < table1NumCols; i++) 
					table1Row[i] = dis1.readInt();
				
				if (table1Row[table1JoinCol] == table1Row[table2JoinCol]) {
					// Writes the combined row of the 2 tables
					for (int i : table1Row) {
						dos.writeInt(i);
						System.out.print(i + " |");
					}
					System.out.println();
				}

			}
		} catch (EOFException e) { // Done reading table1
			
		}
		dos.close();
	}
	
	public void equijoinTwoTables(String tableName, String table1FileName, String table2FileName, int table1NumCols, int table2NumCols, int table1JoinCol, int table2JoinCol) throws IOException {
		// Reading Table1 Data
		DataInputStream dis1 = new DataInputStream(new BufferedInputStream(new FileInputStream(table1FileName)));
		
		// Buffer
		ArrayList<int[]> table1Buffer = new ArrayList<>();
		
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName)));
		resultNumber++;

		try {
			while (true) { // Reads dis1 InputStream
				int[] table1Row = new int[table1NumCols];
				for (int i = 0; i < table1NumCols; i++) 
					table1Row[i] = dis1.readInt();
				table1Buffer.add(table1Row);

				if (table1Buffer.size() < bufferSize) continue;
				
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
									// System.out.print(i + " |");
								}
								for (int i : table2Row) {
									dos.writeInt(i);
									// System.out.print(i + " |");
								}
								// System.out.println();
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
						
						if (table1TempRow[table1JoinCol] == table2Row[table2JoinCol]) {
							// Writes the combined row of the 2 tables
							for (int i : table1TempRow) {
								dos.writeInt(i);
								// System.out.print(i + " |");
							}
							for (int i : table2Row) {
								dos.writeInt(i);
								// System.out.print(i + " |");
							}
							// System.out.println();
							// System.out.println();
						}
					}
				}
				
			} catch (EOFException eof) { // Done reading table2
				
			}
		}
		
		dos.close();
	}
	
	public String noPredicateJoin(String table1FileName, String table2FileName) throws IOException {	
		int table1DotIndex = table1FileName.indexOf('.');
		int table2DotIndex = table2FileName.indexOf('.');
		
		String table1Header = Catalog.getHeader(table1FileName.substring(0, table1DotIndex));
		String table2Header = Catalog.getHeader(table2FileName.substring(0, table2DotIndex));
		
		String[] table1Arr = table1Header.split(",");
		String[] table2Arr = table2Header.split(",");
		
		// Total columns for each table
		int table1NumCols = table1Arr.length;
		int table2NumCols = table2Arr.length;
		
		// Reading Table1 Data
		DataInputStream dis1 = new DataInputStream(new BufferedInputStream(new FileInputStream(table1FileName)));
		
		ArrayList<int[]> table1Buffer = new ArrayList<>();
		
		// DataOutputStream to write the output
		String result = "result" + resultNumber;
		String tableName = result + ".dat";
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName)));
		resultNumber++;
		
		try {
			while (true) { // Reads dis1 InputStream
				int[] table1Row = new int[table1NumCols];
				for (int i = 0; i < table1NumCols; i++) 
					table1Row[i] = dis1.readInt();
				table1Buffer.add(table1Row);

				if (table1Buffer.size() < bufferSize) continue;
				
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

							// Writes the combined row of the 2 tables
							for (int i : table1TempRow) {
								dos.writeInt(i);
							}
							for (int i : table2Row) {
								dos.writeInt(i);
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

						// Writes the combined row of the 2 tables
						for (int i : table1TempRow) {
							dos.writeInt(i);
						}
						for (int i : table2Row) {
							dos.writeInt(i);
						}
						
					}
				}
				
			} catch (EOFException eof) { // Done reading table2
				
			}
		}

		dos.close();
		
		TableMetaData metadata = new TableMetaData(table1Header + "," + table2Header);
		Catalog.addData(result, metadata);
		Catalog.removeData(table1FileName.substring(0, table1FileName.length() - 4));
		Catalog.removeData(table2FileName.substring(0, table2FileName.length() - 4));
		
		return tableName;
		
	}

	public void sumAggregate(String tableFileName, String columnNames) throws IOException {
		//System.out.println(tableFileName + "-------");
		//System.out.println(columnNames + " - sumAggregate - 381");
		String[] columnNameArr = columnNames.split(",");
		int numOfSumCols = columnNameArr.length;
		
		int[] sumColIndices = new int[numOfSumCols]; // column array - which column we are summing
		int[] sums = new int[numOfSumCols]; // sum holder array
		//System.out.println(numOfSumCols + " - numOfSumCols - sumAggregate - 386");
		
		String header = Catalog.getHeader(tableFileName.substring(0, tableFileName.length() - 4));
		String[] headerArr = header.split(",");
		int numOfCols = headerArr.length;
		//System.out.println("numOfCols = " + numOfCols + " - sumAggregate - 392");
		// Finds the index of each column
		for (int i = 0; i < numOfSumCols; i++) {
			sumColIndices[i] = findIndex(headerArr, columnNameArr[i]);
			//System.out.println(sumColIndices[i] + " - sumAggregate - 396");
		}
		
		// Reading Table Data
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tableFileName)));
		
		boolean hasRows = false;

		try {
			while (true) {
				// Stores a row of the data
				int[] row = new int[numOfCols];
				for (int i = 0; i < numOfCols; i++) {
					row[i] = dis.readInt();
				}
				hasRows = true;
				
				for (int i = 0; i < sumColIndices.length; i++) {
					int index = sumColIndices[i];
					sums[i] += row[index];
				}
			}
		} catch (EOFException e) { // Finished reading table
			
		}

		dis.close();
		
		
		PrintStream ps = new PrintStream(new File("sum" + finalNum + ".txt"));
		finalNum++;

		if (hasRows) {
			// Prints out the output
			for (int i = 0; i < sums.length - 1; i++) {
				System.out.print(sums[i] + ",");
				ps.print(sums[i] + ",");
			}
			System.out.println(sums[sums.length - 1]);
			ps.print(sums[sums.length - 1]);
		} else {
			for (int i = 0; i < sums.length - 1; i++)
				System.out.print(",");
			System.out.println();
			//System.out.println("No rows.");
		}
		
		
		this.resultNumber = 0; // resets the result number
		this.finalDeque = new ArrayDeque<>();
		
		Catalog.removeData(tableFileName.substring(0, tableFileName.length() - 4)); // removes the table from the map
	}
	
	public int findIndex(String[] columnNames, String columnName) {
		int i = 0;
		while (i < columnNames.length) {
			if (columnNames[i].equals(columnName)) return i;
			i++;
		}
		return -1;
	}
}