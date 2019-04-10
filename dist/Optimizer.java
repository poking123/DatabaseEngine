
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
	
	public Optimizer() {
		disjointDeque = new ArrayDeque<>();
		predicateJoinQueueMap = new HashMap<>();
		
		bestOrderMap = new HashMap<>();
	}
	
	public Deque<String[]> getDisjointDeque() {
		return this.disjointDeque;
	}
	
	public HashMap<Character, Queue<String[]>> getPredicateJoinQueueMap() {
		return this.predicateJoinQueueMap;
	}
	
	public String estimatePredicates(String fromData, HashMap<Character, ArrayList<int[]>> getTablePredicateMap) {
		// for each table that has an and predicate, update Catalog with the estimate
		HashSet<Character> andDataSet = new HashSet<>(getTablePredicateMap.keySet());
		Iterator<Character> andDataSetItr = andDataSet.iterator();
		
		
		
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
							Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
						}
					} else if (greaterThanValues.size() > 0) {
						if (equalValue <= greaterThanValues.first()) {
							// no rows will be returned
							TableMetaData tmd = new TableMetaData(header);
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
							uniqueCols[i] = (int) Math.round(uniqueCols[i] * percentage);
						}
						uniqueCols[column] = 1;
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
							uniqueCols[i] = (int) Math.round(uniqueCols[i] * percentage);
						}
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
							uniqueCols[i] = (int) Math.round(uniqueCols[i] * percentage);
						}
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
							uniqueCols[i] = (int) Math.round(uniqueCols[i] * percentage);
						}
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
				String table = firstTable + secondTable + "";
				if (firstTable != secondTable) bestOrderMap.put(table, table);
			}
		}
	}
	
	public String selingerAlgorithm(String fromData, AndData andData) {
		HashMap<Character, ArrayList<int[]>> tablePredicateMap = andData.getTablePredicateMap();
		// Calculates new data for tables with predicates
		// and switches values to lowercase in fromData
		String newFromData = estimatePredicates(fromData, tablePredicateMap);
		
		// Loads pairs of 2 into the costMap (passes in both orders)
		loadInPairs(newFromData);
		
		// Selinger's Algorithm for the best join ordering
		System.out.println("fromData: " + newFromData);

		computeBest(newFromData);
		String bestOrder = bestOrderMap.get(newFromData);
		
		return bestOrder;
	}
	
	public String computeBest(String rels) {
		if (bestOrderMap.containsKey(rels))
			return bestOrderMap.get(rels);
		
		int bestCost = Integer.MAX_VALUE;
		String bestOrder = "";
		
		for (char c : rels.toCharArray()) {
			String internalOrder = computeBest(rels.replace(Character.toString(c), ""));
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
		
		bestOrderMap.put(rels, bestOrder);
		return bestOrder;
	}
	
	
	public int cost(String tables) {
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
			String table2FileName = table2 + ".dat";
			
			table2 = Character.toUpperCase(table2);
			
			char table1JoinTable = ' ';
			HashMap<Character, String> equijoinMap = null;
			
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
	
	
	public void optimizeQuery(String fromData, WhereData whereData, AndData andData) {
		
		whereTables = whereData.getTables();

		String bestOrder = selingerAlgorithm(fromData, andData);
		System.out.println("bestOrder is " + bestOrder);
		
		System.exit(0);
		
		
		// Equijoins table names
		// each hashmap contains 2 elements
		// tableName -> tableName.columnNumber
		whereTables = whereData.getTables();
		
		// Match an andTable to equijoins with that table
		HashSet<Character> andTableNames = andData.getTableNames();
		
		Iterator<Character> andItr = andTableNames.iterator();
		while (andItr.hasNext()) { // no more equijoins left
			char predTableName = andItr.next();
			HashSet<Character> includedTables = new HashSet<>();
			includedTables.add(predTableName);
			
			Queue<String[]> predicateQueue = new LinkedList<>(); // queue for equijoins of the predicate table
			boolean leaveLoop = false;
			
			// Keep on iterating over whereTables
			while (!whereTables.isEmpty() && !leaveLoop) { // Still equijoins left
				int includedTablesBeforeSize = includedTables.size();
				
				Iterator<HashMap<Character, String>> whereTablesItr = whereTables.iterator();
				while (whereTablesItr.hasNext()) {
					HashMap<Character, String> equijoinTables = whereTablesItr.next(); // map of table names to join columns
					HashSet<Character> equijointableNames = new HashSet<>(equijoinTables.keySet());
					
					char containedTableChar = containsPredicateTable(includedTables, equijointableNames);
					if (containedTableChar != ' ') { // equijoin has predicate table in it
						// predicateQueryData has 
						// index 0 - predicateTableColumnName
						// index 1 - otherTableName
						// index 2 - otherTableColumnName
						String[] predicateQueryData = new String[3];
						// Iterate over the set
						Iterator<Character> equijointableNamesItr = equijointableNames.iterator();
						while (equijointableNamesItr.hasNext()) { // 2 iterations - fills out predicateQueryData
							char equijointableName = equijointableNamesItr.next();
							if (equijointableName == containedTableChar) { // found the contained character
								predicateQueryData[0] = equijoinTables.get(equijointableName); // fill in the table column name
							} else { // other table
								includedTables.add(equijointableName);
								predicateQueryData[1] = Character.toString(equijointableName); // other table name
								predicateQueryData[2] = equijoinTables.get(equijointableName); // other table column name
							}
						}
						// Adds predicate query data to queue
						predicateQueue.add(predicateQueryData);
						
						// Removes from whereTables
						whereTablesItr.remove();
					}
				}
				
				int includedTablesAfterSize = includedTables.size();
				if (includedTablesBeforeSize == includedTablesAfterSize)
					leaveLoop = true;
			}
			predicateJoinQueueMap.put(predTableName, predicateQueue); // add to map (character to queue of joins)
		}
		
		// Put rest of the equijoins into the disjointDeque
		for (HashMap<Character, String> map : whereTables)
			addToDisjointDeque(map);
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