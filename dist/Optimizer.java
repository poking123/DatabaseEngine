
import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeSet;
import java.util.Arrays;

public class Optimizer {
    
	private Deque<String[]> disjointDeque;
	private HashMap<Character, Queue<String[]>> predicateJoinQueueMap;
	
	private HashMap<String, String> bestOrderMap;
	private ArrayList<HashMap<Character, String>> whereTables;

	private Queue<Queue<RAOperation>> tablesQueue;
	private Queue<Queue<Predicate>> predicatesQueue;
	
	private Queue<Predicate> finalPredicateQueue;
	private int[] columnsToSum;
	
	public Optimizer() {
		disjointDeque = new ArrayDeque<>();
		predicateJoinQueueMap = new HashMap<>();
		
		bestOrderMap = new HashMap<>();
		whereTables = new ArrayList<>();
		tablesQueue = new LinkedList<>();
		predicatesQueue = new LinkedList<>();
		
		finalPredicateQueue = new LinkedList<>();
		columnsToSum = new int[1];
	}
	
	public Queue<Queue<RAOperation>> getTablesQueue() {
		return this.tablesQueue;
	}

	public Queue<Queue<Predicate>> getPredicatesQueue() {
		return this.predicatesQueue;
	}
	
	public int[] getColumnsToSum() {
		return this.columnsToSum;
	}
	
	public Queue<Predicate> getFinalPredicateQueue() {
		return this.finalPredicateQueue;
	}
	
	public HashMap<Character, Queue<String[]>> getPredicateJoinQueueMap() {
		return this.predicateJoinQueueMap;
	}
	
	public String estimatePredicates(String fromData, HashMap<Character, ArrayList<int[]>> getTablePredicateMap) {
		// for each table that has an and predicate, update Catalog with the estimate
		HashSet<Character> andDataSet = new HashSet<>(getTablePredicateMap.keySet());
		Iterator<Character> andDataSetItr = andDataSet.iterator();
		
		
		// Goes through all the tables with predicates
		while (andDataSetItr.hasNext()) {
			char tableName = andDataSetItr.next();
			
			String header = Catalog.getHeader(tableName + ".dat");
			
			ArrayList<int[]> predicateDataList = getTablePredicateMap.get(tableName);
			// condense predicates
			// int[]
			// index 0 - column
			// index 1 - operator
			// index 2 - compare value
			
			int[] operatorCounter = new int[3];
			// index 0 - =
			// index 1 - <
			// index 2 - >
			
			TreeSet<Integer> equalValues = new TreeSet<>();
			TreeSet<Integer> lessThanValues = new TreeSet<>();
			TreeSet<Integer> greaterThanValues = new TreeSet<>();
			
			Iterator<int[]> predicateDataItr = predicateDataList.iterator();
			while (predicateDataItr.hasNext()) {
				int[] data = predicateDataItr.next();
				// column
				int column = data[0];
				// counts operator
				int operator = data[1];
				operatorCounter[operator]++;
				// compare value
				int compareValue = data[2];
				switch (operator) {
					case 0:
						equalValues.add(compareValue);
						break;
					case 1:
						lessThanValues.add(compareValue);
						break;
					case 2:
						greaterThanValues.add(compareValue);
						break;
				}
				
				if (equalValues.size() > 1) {
					// no rows will be returned
					TableMetaData tmd = new TableMetaData(header);
					Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
				} else if (equalValues.size() == 1) { // One equals value
					int equalValue = equalValues.first();
					if (lessThanValues.size() > 0) {
						if (equalValue >= lessThanValues.last()) {
							// no rows will be returned
							TableMetaData tmd = new TableMetaData(header);
							int numOfCols = Catalog.getColumns(tableName + ".dat");
							tmd.setColumns(numOfCols);
							Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
						}
					} else if (greaterThanValues.size() > 0) {
						if (equalValue <= greaterThanValues.first()) {
							// no rows will be returned
							TableMetaData tmd = new TableMetaData(header);
							int numOfCols = Catalog.getColumns(tableName + ".dat");
							tmd.setColumns(numOfCols);
							Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
						}
					} else { // only one equals value
						// estimate number of rows
						TableMetaData tmd = new TableMetaData(header);
						int numOfRows = Catalog.getRows(tableName + ".dat");
						int uniqueValues = Catalog.getUnique(tableName + ".dat", column);
						
						int estimateRows = numOfRows / uniqueValues;
						tmd.setRows(estimateRows); // sets the number of rows
						
						double percentage = (double) estimateRows / numOfRows;
						
						int[] uniqueCols = Catalog.getUniqueColumns(tableName + ".dat");
						uniqueCols = Arrays.copyOf(uniqueCols, uniqueCols.length);
						
						for (int i = 0; i < uniqueCols.length; i++) {
							int value = (int) Math.round(uniqueCols[i] * percentage);
							if (value != 0) {
								uniqueCols[i] = value;
							} else {
								uniqueCols[i] = 1;
							}
						}
						uniqueCols[column] = 1;

						int numOfCols = Catalog.getColumns(tableName + ".dat");
						tmd.setColumns(numOfCols);

						tmd.setUnique(uniqueCols);
						
						Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
					}
				} else { // no equals
					if (lessThanValues.size() > 0 && greaterThanValues.size() > 0) {
						// estimate number of rows
						TableMetaData tmd = new TableMetaData(header);
						int numOfRows = Catalog.getRows(tableName + ".dat");
						int min = Catalog.getMin(tableName + ".dat", column);
						int max = Catalog.getMax(tableName + ".dat", column);
						int uniqueValues = Catalog.getUnique(tableName + ".dat", column);
						
						int estimateRows = estimatePredicateRows(numOfRows, min, max, uniqueValues, 1, lessThanValues.last()) * estimatePredicateRows(numOfRows, min, max, uniqueValues, 2, greaterThanValues.first()) / numOfRows;
						tmd.setRows(estimateRows); // sets the number of rows
						
						double percentage = (double) estimateRows / numOfRows;
						
						int[] uniqueCols = Catalog.getUniqueColumns(tableName + ".dat");
						uniqueCols = Arrays.copyOf(uniqueCols, uniqueCols.length);
						
						for (int i = 0; i < uniqueCols.length; i++) {
							int value = (int) Math.round(uniqueCols[i] * percentage);
							if (value != 0) {
								uniqueCols[i] = value;
							} else {
								uniqueCols[i] = 1;
							}
						}

						int numOfCols = Catalog.getColumns(tableName + ".dat");
						tmd.setColumns(numOfCols);

						tmd.setUnique(uniqueCols);
						
						Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
					} else if (lessThanValues.size() > 0) {
						// estimate number of rows
						TableMetaData tmd = new TableMetaData(header);
						int numOfRows = Catalog.getRows(tableName + ".dat");
						int min = Catalog.getMin(tableName + ".dat", column);
						int max = Catalog.getMax(tableName + ".dat", column);
						int uniqueValues = Catalog.getUnique(tableName + ".dat", column);
						
						int estimateRows = estimatePredicateRows(numOfRows, min, max, uniqueValues, 1, lessThanValues.last());
						tmd.setRows(estimateRows); // sets the number of rows
						
						double percentage = (double) estimateRows / numOfRows;
						
						int[] uniqueCols = Catalog.getUniqueColumns(tableName + ".dat");
						uniqueCols = Arrays.copyOf(uniqueCols, uniqueCols.length);
						
						for (int i = 0; i < uniqueCols.length; i++) {
							int value = (int) Math.round(uniqueCols[i] * percentage);
							if (value != 0) {
								uniqueCols[i] = value;
							} else {
								uniqueCols[i] = 1;
							}
						}

						int numOfCols = Catalog.getColumns(tableName + ".dat");
						tmd.setColumns(numOfCols);

						tmd.setUnique(uniqueCols);
						
						Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
					} else if (greaterThanValues.size() > 0) {
						// estimate number of rows
						TableMetaData tmd = new TableMetaData(header);
						int numOfRows = Catalog.getRows(tableName + ".dat");
						int min = Catalog.getMin(tableName + ".dat", column);
						int max = Catalog.getMax(tableName + ".dat", column);
						int uniqueValues = Catalog.getUnique(tableName + ".dat", column);
						
						int estimateRows = estimatePredicateRows(numOfRows, min, max, uniqueValues, 1, greaterThanValues.first());
						tmd.setRows(estimateRows); // sets the number of rows
						
						double percentage = (double) estimateRows / numOfRows;
						
						int[] uniqueCols = Catalog.getUniqueColumns(tableName + ".dat");
						uniqueCols = Arrays.copyOf(uniqueCols, uniqueCols.length);
						
						for (int i = 0; i < uniqueCols.length; i++) {
							int value = (int) Math.round(uniqueCols[i] * percentage);
							if (value != 0) {
								uniqueCols[i] = value;
							} else {
								uniqueCols[i] = 1;
							}
						}

						int numOfCols = Catalog.getColumns(tableName + ".dat");
						tmd.setColumns(numOfCols);

						tmd.setUnique(uniqueCols);
						
						Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
					}
				}	
			}
			
			// and lowercase the table name 
			fromData = fromData.replace(tableName, Character.toLowerCase(tableName));
		}
		
		return fromData;
	}
	
	public int estimatePredicateRows(int tableNumRows, int min, int max, int uniqueColValues, int operator, int compareValue) {
		switch (operator) {
			case 0: // equals
				return tableNumRows / uniqueColValues;
			case 1: // less than
				return tableNumRows * (max - compareValue) / (max - min);
			case 2: // greater than
				return tableNumRows * (compareValue - min) / (max - min);
			default:
				return 0; // should not get here
		}
	}

	public void loadInPairs(String fromData) {
		for (char firstTable : fromData.toCharArray()) {
			for (char secondTable : fromData.toCharArray()) {
				String table = firstTable + "" + secondTable;
				
				if (firstTable != secondTable) {
					bestOrderMap.put(table, table);
				} 
			}
		}
		System.out.println(bestOrderMap);
	}
	
	public String selingerAlgorithm(String fromData, AndData andData) {
		HashMap<Character, ArrayList<int[]>> tablePredicateMap = andData.getTablePredicateMap();
		// Calculates new data for tables with predicates
		// and switches values to lowercase in fromData
		String newFromData = estimatePredicates(fromData, tablePredicateMap);

		// Loads pairs of 2 into the costMap (passes in both orders)
		loadInPairs(fromData); // loads in the all capital version
		
		// Selinger's Algorithm for the best join ordering
		System.out.println("fromData: " + newFromData);

		String bestOrder = computeBest(newFromData);
		
		return bestOrder;
	}
	
	public String computeBest(String rels) {
		System.out.println("rels is " + rels);
		if (bestOrderMap.containsKey(rels))
			return bestOrderMap.get(rels);
		
		int bestCost = Integer.MAX_VALUE;
		String bestOrder = "";
		
		for (char c : rels.toCharArray()) {
			System.out.println("The char c is " + c);
			String internalOrder = computeBest(rels.replace(Character.toString(c), ""));
			System.out.println("internalOrder is " + internalOrder);
			int cost1 = cost(c + internalOrder);
			int cost2 = cost(internalOrder + c);
			String internalBestOrder = "";
			int internalBestCost = -1;
			if (cost1 < cost2) { // checks the two costs, and saves the better order
				internalBestOrder = c + internalOrder;
				internalBestCost = cost1;
			} else {
				internalBestOrder = internalOrder + c;
				internalBestCost = cost2;
			}
			
			// checks if internal best is better than best cost
			// reassigns best order if so
			if (internalBestCost < bestCost) { 
				bestCost = internalBestCost;
				bestOrder = internalBestOrder;
			}
		}
		
		System.out.println("Last rels is " + rels);
		bestOrderMap.put(rels, bestOrder);
		System.out.println(bestOrderMap.containsKey(rels));
		System.out.println("result: " + bestOrderMap.get(rels));
		System.out.println(bestOrderMap);
		return bestOrder;
	}
	
	
	public int cost(String tables) {
		System.out.println("Calculating costs of " + tables);
		// String tables is just a string, where each character is a table
		char[] tablesCharArray = tables.toCharArray();
		String table1 = Character.toString(tablesCharArray[0]);
		String table1FileName = table1 + ".dat";
		
		
		int table1NumRows = Catalog.getRows(table1FileName);
		int table1NumCols = Catalog.getColumns(table1FileName);

		int[] table1UniqueColumns = Arrays.copyOf(Catalog.getUniqueColumns(table1FileName), table1NumCols);
		
		StringBuilder table1HeaderSB = new StringBuilder(Catalog.getHeader(table1FileName));

		table1 = table1.toUpperCase();
		
		// goes through table string
		for (int i = 1; i < tablesCharArray.length; i++) {
			char table2 = tablesCharArray[i];
			String table2FileName = table2 + ".dat"; // uses this to get metadata
			
			table2 = Character.toUpperCase(table2);
			
			char table1JoinTable = ' ';
			HashMap<Character, String> equijoinMap = null;

			System.out.println("i is " + i);
			System.out.println("table1 is " + table1);
			System.out.println("table2 is " + table2);
			
			// returns the map of the equijoin
			// and the char of the table in table1 that is a part of the join
			for (char c : table1.toCharArray()) {
				char actualTable = Character.toUpperCase(c);
				for (HashMap<Character, String> map : whereTables) {
					if (map.containsKey(actualTable) && map.containsKey(table2)) {
						table1JoinTable = actualTable;
						equijoinMap = map;
					}
				}
			}
			
			
			if (equijoinMap == null) { // no equijoin, so cost is high
				return Integer.MAX_VALUE;
			}
			
			
			// use header to get the number of unique values in the right columns
			String[] table1HeaderArr = table1HeaderSB.toString().split(",");
			String table1JoinColName = equijoinMap.get(table1JoinTable);

			String table2Header = Catalog.getHeader(table2FileName);
			String[] table2HeaderArr = table2Header.split(",");
			String table2JoinColName = equijoinMap.get(table2);
			
			// gets integer join columns
			int table1JoinCol = findIndex(table1HeaderArr, table1JoinColName);
			int table2JoinCol = findIndex(table2HeaderArr, table2JoinColName);
			
			int[] table2UniqueColumns = Arrays.copyOf(Catalog.getUniqueColumns(table2FileName), table2HeaderArr.length);
			
			int table1NumUniqueCol = table1UniqueColumns[table1JoinCol];
			int table2NumUniqueCol = Catalog.getUnique(table2FileName, table2JoinCol);
			
			int minUnique = table1NumUniqueCol < table2NumUniqueCol ? table1NumUniqueCol : table2NumUniqueCol;
			
			// calculates meta data for current table1
			int table2NumRows = Catalog.getRows(table2FileName);
			int maxRows = table1NumRows * table2NumRows;
			// estimated rows for joined table
			table1NumRows = table1NumRows * table2NumRows * minUnique / table1NumUniqueCol / table2NumUniqueCol; // updates number of rows
			
			
			
			if (table1HeaderSB.toString().indexOf(table2JoinCol) < 0) { // table2 join col not is already in table1 header
				table1HeaderSB.append("," + table2Header); // updates table1 header
				
				table1UniqueColumns = combineRows(table1UniqueColumns, table2UniqueColumns);// updates unique columns
			}
			
			// percentage to decrease number of unique values
			double percentage = (double) table1NumRows / maxRows;

			for (int index = 0; index < table1UniqueColumns.length; index++) {
				int value = (int) Math.round(table1UniqueColumns[index] * percentage);
				if (value == 0) { // makes sure no value is 0 so we don't divide by 0
					table1UniqueColumns[index] = 1;
				} else {
					table1UniqueColumns[index] = value;
				}
			}
			
			// updates table1 name
			table1 += table2; 
			
		}
		
		return table1NumRows;
	}
	
	public int findIndex(String[] columnNames, String columnName) {
		int i = 0;
		while (i < columnNames.length) {
			if (columnNames[i].equals(columnName)) return i;
			i++;
		}
		return -1;
	}
	
	public int[] combineRows(int[] row1, int[] row2) {
		int[] combinedRow = new int[row1.length + row2.length];
		int i = 0;
		int totalIndex = 0;
		
		while (i < row1.length) {
			combinedRow[totalIndex] = row1[totalIndex];
			i++;
			totalIndex++;
		}
		
		i = 0;
		
		while (i < row2.length) {
			combinedRow[totalIndex] = row2[i];
			i++;
			totalIndex++;
		}
		
		return combinedRow;
	}
	
	public int getJoinCol(HashMap<Character, String> equijoinMap, char tableName) {
		String tableNameAndColumn = equijoinMap.get(tableName);
		int cIndex = tableNameAndColumn.indexOf(".c");
		int joinCol = Integer.parseInt(tableNameAndColumn.substring(cIndex + 2, tableNameAndColumn.length()));
		
		return joinCol;
	}
	
	
	public void optimizeQuery(String columnNames, String fromData, WhereData whereData, AndData andData) throws FileNotFoundException {
		
		HashMap<Character, ArrayList<int[]>> tablePredicateMap = andData.getTablePredicateMap();

		System.out.println("columnNames is " + columnNames);
		System.out.println("fromData is " + fromData);
		System.out.println("tablePredicateMap is " + tablePredicateMap);
		
		Queue<RAOperation> tableQueue = new LinkedList<>();
		Queue<Predicate> predicateQueue = new LinkedList<>();
		
		// whereTables is an arraylist of maps from table -> table.columnNumber
		whereTables = whereData.getTables();

		HashMap<Character, String> firstJoin = whereTables.get(0);
		whereTables.remove(0);
		
		
		HashSet<Character> joinedTables = new HashSet<>(firstJoin.keySet());
		Iterator<Character> joinedTablesItr = joinedTables.iterator();
		// only two tables
		char firstTable = joinedTablesItr.next();
		char secondTable = joinedTablesItr.next();
		
		// Adds tables to predicateQueue
		tableQueue.add(new Scan(firstTable + ".dat"));
		tableQueue.add(new Scan(secondTable + ".dat"));
		
		// Check for predicates and adds to predicateQueue
		if (tablePredicateMap.containsKey(firstTable)) {
			ArrayList<int[]> predData = tablePredicateMap.get(firstTable);
			predicateQueue.add(new FilterPredicate(predData));
		}
		if (tablePredicateMap.containsKey(secondTable)) {
			ArrayList<int[]> predData = tablePredicateMap.get(secondTable);
			predicateQueue.add(new FilterPredicate(predData));
		}
		
		String firstJoinColumn = firstJoin.get(firstTable);
		String secondJoinColumn = firstJoin.get(secondTable);
		
		// makes the header
		StringBuilder header = new StringBuilder();
		StringBuilder overallHeader = new StringBuilder();
		String firstHeader = Catalog.getHeader(firstTable + ".dat");
		String secondHeader = Catalog.getHeader(secondTable + ".dat");
		header.append(firstHeader + "," + secondHeader);
		
		int firstTableJoinCol = findIndex(firstHeader.split(","), firstJoinColumn);
		int secondTableJoinCol = findIndex(secondHeader.split(","), secondJoinColumn);
		
		// Adds equijoin to predicateQueue
		predicateQueue.add(new EquijoinPredicate(firstTableJoinCol, secondTableJoinCol, true));
		
		boolean lastIsDisjoint = false;
		// goes through whereTables and joins table to the table we already made
		while (!whereTables.isEmpty()) {
			lastIsDisjoint = false;

			int beforeSize = joinedTables.size();
			Iterator<HashMap<Character, String>> whereTablesItr = whereTables.iterator();
			while (whereTablesItr.hasNext()) {
				System.out.println("In Where Tables");
				System.out.println("whereTables is " + whereTables);
				HashMap<Character, String> tempMap = whereTablesItr.next();
				HashSet<Character> tablesCharSet = new HashSet<>(tempMap.keySet());
				
				Iterator<Character> tablesCharSetItr = tablesCharSet.iterator();
				char tempTable1 = tablesCharSetItr.next();
				char tempTable2 = tablesCharSetItr.next();
				
				// join columns (Strings)
				firstJoinColumn = tempMap.get(tempTable1);
				secondJoinColumn = tempMap.get(tempTable2);
				
				// header of the already made table
				String[] firstTableHeaderArr = header.toString().split(",");
				
				// if both tables are in the already made table, do a 1 table equijoin
				boolean containsTable1 = joinedTables.contains(tempTable1);
				boolean containsTable2 = joinedTables.contains(tempTable2);
				if (containsTable1 && containsTable2) {
					System.out.println("Contains both");
					// add fake table to tableQueue
					tableQueue.add(new Scan("FakeTable"));
					// add one table equijoin to predicateQueue
					firstTableJoinCol = findIndex(firstTableHeaderArr, firstJoinColumn);
					secondTableJoinCol = findIndex(firstTableHeaderArr, secondJoinColumn);
					predicateQueue.add(new EquijoinPredicate(firstTableJoinCol, secondTableJoinCol, false));
					whereTablesItr.remove();
				} else if (containsTable1) { // contains table1, but not table2
					System.out.println("contains table 1");
					// adds table2 to joinedTables
					joinedTables.add(tempTable2);
					// adds table2 to tableQueue
					tableQueue.add(new Scan(tempTable2 + ".dat"));
					
					// Check for predicates and adds to predicateQueue
					if (tablePredicateMap.containsKey(tempTable2)) {
						ArrayList<int[]> predData = tablePredicateMap.get(tempTable2);
						predicateQueue.add(new FilterPredicate(predData));
					}

					// updates header
					String tempHeader = Catalog.getHeader(tempTable2 + ".dat");
					header.append("," + tempHeader);
					
					// adds two table equijoin to predicateQueue
					firstTableJoinCol = findIndex(firstTableHeaderArr, firstJoinColumn);
					secondTableJoinCol = findIndex(tempHeader.split(","), secondJoinColumn);
					predicateQueue.add(new EquijoinPredicate(firstTableJoinCol, secondTableJoinCol, true));
					whereTablesItr.remove();
				} else if (containsTable2) {
					System.out.println("contains table 2");
					// adds table1 to joinedTables
					joinedTables.add(tempTable1);
					// adds table1 to tableQueue
					tableQueue.add(new Scan(tempTable1 + ".dat"));
					
					// Check for predicates and adds to predicateQueue
					if (tablePredicateMap.containsKey(tempTable1)) {
						ArrayList<int[]> predData = tablePredicateMap.get(tempTable1);
						predicateQueue.add(new FilterPredicate(predData));
					}
					
					// updates header
					String tempHeader = Catalog.getHeader(tempTable1 + ".dat");
					header.append("," + tempHeader);
					
					// adds two table equijoin to predicateQueue
					firstTableJoinCol = findIndex(firstTableHeaderArr, secondJoinColumn);
					secondTableJoinCol = findIndex(tempHeader.split(","), firstJoinColumn);
					predicateQueue.add(new EquijoinPredicate(firstTableJoinCol, secondTableJoinCol, true));
					whereTablesItr.remove();
				}
				// else (disjoint join, so skip)
			}
			int afterSize = joinedTables.size();
			if (beforeSize == afterSize) {
				lastIsDisjoint = true;

				tablesQueue.add(new LinkedList<>(tableQueue));
				predicatesQueue.add(new LinkedList<>(predicateQueue));
				tableQueue.clear();
				predicateQueue.clear();
				
				firstJoin = whereTables.get(0);
				joinedTables = new HashSet<>(firstJoin.keySet());
				whereTables.remove(0);
				
				// gets the table names
				joinedTablesItr = joinedTables.iterator();
				// only two tables
				firstTable = joinedTablesItr.next();
				secondTable = joinedTablesItr.next();
				
				// adds to overall header
				if (overallHeader.length() > 0) {
					overallHeader.append("," + header.toString());
				} else {
					overallHeader.append(new StringBuilder(header));
				}
				
				//resets header
				header.setLength(0);
				firstHeader = Catalog.getHeader(firstTable + ".dat");
				secondHeader = Catalog.getHeader(secondTable + ".dat");
				header.append(firstHeader + "," + secondHeader);
				
				
				// join columns (String)
				firstJoinColumn = firstJoin.get(firstTable);
				secondJoinColumn = firstJoin.get(secondTable);
				// join columns (int)
				firstTableJoinCol = findIndex(firstHeader.split(","), firstJoinColumn);
				secondTableJoinCol = findIndex(secondHeader.split(","), secondJoinColumn);
				
				// Check for predicates and adds to predicateQueue
				if (tablePredicateMap.containsKey(firstTable)) {
					ArrayList<int[]> predData = tablePredicateMap.get(firstTable);
					predicateQueue.add(new FilterPredicate(predData));
				}
				if (tablePredicateMap.containsKey(secondTable)) {
					ArrayList<int[]> predData = tablePredicateMap.get(secondTable);
					predicateQueue.add(new FilterPredicate(predData));
				}
				
				// Adds equijoin to predicateQueue
				predicateQueue.add(new EquijoinPredicate(firstTableJoinCol, secondTableJoinCol, true));

			}
		}
		
		if (!lastIsDisjoint) {
			tablesQueue.add(new LinkedList<>(tableQueue));
			predicatesQueue.add(new LinkedList<>(predicateQueue));

			// adds to overall header
			if (overallHeader.length() > 0) {
				overallHeader.append("," + header.toString());
			} else {
				overallHeader.append(new StringBuilder(header));
			}
			
			// adds disjoint equijoins to the finalPredicateQueue
			for (int i = 0; i < predicatesQueue.size() - 1; i++) {
				finalPredicateQueue.add(new EquijoinPredicate(-3, -3, true)); // adds cross products for the disjoint join
			}
		}
		
		
		System.out.println("Overall Header:");
		System.out.println(overallHeader.toString());
		System.out.println();
		String[] overallHeaderArr = overallHeader.toString().split(",");
		String[] columnNamesArr = columnNames.split(",");
		columnsToSum = new int[columnNamesArr.length];
		
		for (int i = 0; i < columnNamesArr.length; i++) {
			columnsToSum[i] = findIndex(overallHeaderArr, columnNamesArr[i]);
		}

//		String bestOrder = selingerAlgorithm(fromData, andData);
//		System.out.println("bestOrder is " + bestOrder);
//
//		
//
//		HashSet<Character> includedTables = new HashSet<>();
//
//		char[] bestOrderCharArr = bestOrder.toCharArray();
//		
//		char firstChar = bestOrderCharArr[0];
//		includedTables.add(firstChar);

//		String header = Catalog.getHeader(firstChar + ".dat");
//		String[] headerArr = header.split(",");

		// for (int i = 1; i < bestOrderCharArr.length; i++) {
		// 	char otherJoinTable = ' ';
		// 	char table1 = bestOrderCharArr[i];
		// 	Iterator<HashMap<Character, String>> whereTablesItr = whereTable.iterator();
		// 	boolean exitLoop = false;
		// 	while (whereTablesItr.hasNext() && !exitLoop) {
		// 		HashMap<Character, String> joinMap = whereTablesItr.next();
		// 		Iterator<Character> includedTablesItr = includedTables.iterator();
		// 		while (includedTablesItr.hasNext() && !exitLoop) {
		// 			char table2 = includedTablesItr.next();
		// 			if (joinMap.containsKey(table1) && joinMap.containsKey(table1)) {
		// 				otherJoinTable = table1;
		// 				exitLoop = true;
		// 			}
		// 		}
		// 	}

		// 	if (exitLoop == false) { // other table found

		// 	} else { // no join condition found - cross product

		// 	}


		// 	if (Character.isLowerCase(c)) {
		// 		char uppercase = Character.toUpperCase(c);
		// 		List<int[]> predicates = tablePredicateMap.get(uppercase);
		// 		Predicate filterPredicate = new FilterPredicate(predicates);

		// 		tableQueue.add(new Scan(uppercase + ".dat"));
		// 		predicateQueue.add(filterPredicate);
		// 	} else {

		// 	}
		// }
	}
	
	public char containsPredicateTable(HashSet<Character> includedTables, HashSet<Character> equijointableNames) {
		Iterator<Character> itr = equijointableNames.iterator();
		while (itr.hasNext()) {
			char tableName = itr.next();
			if (includedTables.contains(tableName)) return tableName;
		}
		return ' ';
	}
	
	public void addToDisjointDeque(HashMap<Character, String> map) {
		HashSet<Character> tables = new HashSet<>(map.keySet());
		Iterator<Character> itr = tables.iterator();
		int tableNameIndex = 0;
		int tableColumnNameIndex = 1;
		
		// index 0 - table1 name
		// index 1 - table1 column name
		// index 2 - table2 name
		// index 3 - table2 column name
		String[] equijoinData = new String[4];
		while (itr.hasNext()) {
			char tableName = itr.next();
			equijoinData[tableNameIndex] = Character.toString(tableName) + ".dat"; // adds table name
			tableNameIndex += 2;
			
			equijoinData[tableColumnNameIndex] = map.get(tableName); // adds join column name
			tableColumnNameIndex += 2;
		}
		disjointDeque.push(equijoinData);
	}
}