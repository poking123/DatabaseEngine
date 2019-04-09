
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class Optimizer {
    
	private Deque<String[]> disjointDeque;
	private HashMap<Character, Queue<String[]>> predicateJoinQueueMap;
	
	private HashMap<String, Integer> costMap;
	private ArrayList<HashMap<Character, String>> whereTables;
	
	public Optimizer() {
		disjointDeque = new ArrayDeque<>();
		predicateJoinQueueMap = new HashMap<>();
		
		costMap = new HashMap<>();
	}
	
	public Deque<String[]> getDisjointDeque() {
		return this.disjointDeque;
	}
	
	public HashMap<Character, Queue<String[]>> getPredicateJoinQueueMap() {
		return this.predicateJoinQueueMap;
	}

	public void loadInPairs(ArrayList<Character> fromData) {
		for (char firstTable : fromData) {
			for (char secondTable : fromData) {
				String table = firstTable + secondTable + "";
				if (firstTable != secondTable) costMap.put(table, cost(table));
			}
		}
	}
	
	public String selingerAlgorithm(ArrayList<Character> fromData, AndData andData) {
		// Calculates new data for tables with predicates
		// and switches values to lowercase in fromData
		
		
		// Loads pairs of 2 into the costMap (passes in both orders)
		loadInPairs(fromData);
		
		return null;
	}
	
	public int cost(String tables) {
		// String tables is just a string, where each character is a table
		
		char[] tablesCharArray = tables.toCharArray();
		char table1 = tablesCharArray[0];
		String table1FileName = table1 + ".dat";
		
		int table1NumRows = Catalog.getRows(table1FileName);
		int[] table1UniqueColumns = Catalog.getUniqueColumns(table1FileName);
		StringBuilder table1Header = new StringBuilder(Catalog.getHeader(table1FileName));
		
		
		for (int i = 1; i < tablesCharArray.length; i++) {
			char table2 = tablesCharArray[i];
			String table2FileName = table2 + ".dat";
			HashMap<Character, String> equijoinMap = returnEquijoinMap(table1, table2);
			if (equijoinMap == null) { // no equijoin, so cost is high
				return Integer.MAX_VALUE;
			}

			int table2NumRows = Catalog.getRows(table2FileName);
			
			String[] table2HeaderArr = Catalog.getHeader(table2FileName).split(",");
			String table2JoinColName = equijoinMap.get(table2);
			
			int[] table2UniqueColumns = Catalog.getUniqueColumns(table2FileName);
			
			
			
			//int table1NumUniqueCol = table1UniqueColumns[table1JoinCol];
			//int table2NumUniqueCol = Catalog.getUnique(table2FileName, table2JoinCol);
			
			//int minUnique = table1NumUniqueCol < table2NumUniqueCol ? table1NumUniqueCol : table2NumUniqueCol;
			
			// calculates meta data for current table1
			
			// estimated rows for joined table
			//table1NumRows = table1NumRows * table2NumRows * minUnique / table1NumUniqueCol / table2NumUniqueCol;
			
			// unique
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
	
	public HashMap<Character, String> returnEquijoinMap(char table1, char table2) {
		for (HashMap<Character, String> map : whereTables) {
			if (map.containsKey(table1) && map.containsKey(table2)) {
				return map;
			}
		}
		
		return null; // no equijoin
	}
	
	public int getJoinCol(HashMap<Character, String> equijoinMap, char tableName) {
		String tableNameAndColumn = equijoinMap.get(tableName);
		int cIndex = tableNameAndColumn.indexOf(".c");
		int joinCol = Integer.parseInt(tableNameAndColumn.substring(cIndex + 2, tableNameAndColumn.length()));
		
		return joinCol;
	}
	
	
	public void optimizeQuery(ArrayList<Character> fromData, WhereData whereData, AndData andData) {
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