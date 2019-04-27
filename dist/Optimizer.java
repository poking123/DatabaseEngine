
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
    
	private HashMap<Character, Queue<String[]>> predicateJoinQueueMap;
	
	private HashMap<String, String> bestOrderMap;
	private ArrayList<HashMap<Character, String>> whereTables;
	private ArrayList<HashMap<Character, String>> tempWhereTables;

	private Queue<Queue<RAOperation>> tablesQueue;
	private Queue<Queue<Predicate>> predicatesQueue;
	private Queue<Queue<Boolean>> switchesQueue;
	
	private Queue<Predicate> finalPredicateQueue;
	private int[] columnsToSum;

	private HashMap<Character, ArrayList<int[]>> tablePredicateMap;

	private HashMap<Character, HashSet<Character>> possibleJoinsMap;

	
	public Optimizer() {
		predicateJoinQueueMap = new HashMap<>();
		
		bestOrderMap = new HashMap<>();
		whereTables = new ArrayList<>();
		tablesQueue = new LinkedList<>();
		predicatesQueue = new LinkedList<>();
		
		finalPredicateQueue = new LinkedList<>();
		columnsToSum = new int[1];

		switchesQueue = new LinkedList<>();
	}
	
	public Queue<Queue<RAOperation>> getTablesQueue() {
		return this.tablesQueue;
	}

	public Queue<Queue<Predicate>> getPredicatesQueue() {
		return this.predicatesQueue;
	}

	public Queue<Queue<Boolean>> getswitchesQueue() {
		return this.switchesQueue;
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

	public void addPredicateData(HashMap<Integer, ArrayList<int[]>> columnToPredicate, int[] predicateData) {
		// 0 - column
		// 1 - operator
		// 2 - compareValue
		int column = predicateData[0];
		int operator = predicateData[1];
		int compareValue = predicateData[2];

		ArrayList<int[]> columnPredDataList = new ArrayList<>();
		if (columnToPredicate.containsKey(operator)) {
			columnPredDataList = columnToPredicate.get(column);
		}
		columnPredDataList.add(new int[]{operator, compareValue});

		columnToPredicate.put(column, columnPredDataList);
	}

	public void reducePredicates(HashMap<Integer, ArrayList<int[]>> columnToPredicate) {
		HashSet<Integer> columns = new HashSet<>(columnToPredicate.keySet());
		Iterator<Integer> columnsItr = columns.iterator();
		while (columnsItr.hasNext()) {
			ArrayList<int[]> columnPredDataList = columnToPredicate.get(columnsItr.next());
			if (columnPredDataList.size() > 1) {
				reducePredicate(columnPredDataList);
			}
		}
	}

	public void reducePredicate(ArrayList<int[]> columnPredDataList) {
		TreeSet<Integer> equalValues = new TreeSet<>();
		TreeSet<Integer> lessThanValues = new TreeSet<>();
		TreeSet<Integer> greaterThanValues = new TreeSet<>();

		for (int[] predData : columnPredDataList) {
			int operator = predData[0];
			int compareValue = predData[1];
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
		}
		columnPredDataList.clear();

		boolean hasLessThanValues = lessThanValues.size() > 0;
		boolean hasGreaterThanValues = greaterThanValues.size() > 0;

		if (equalValues.size() > 1) {
			return;
		} else if (equalValues.size() == 1) { // one equal sign
			int equalValue = equalValues.first();
			if (hasLessThanValues && hasGreaterThanValues) {
				int leastLessThanValue = lessThanValues.first();
				int greatestGreaterThanValue = greaterThanValues.last();
				if (!(equalValue < leastLessThanValue && equalValue > greatestGreaterThanValue)) {
					return;
				}
				return;
			} else if (hasLessThanValues) {
				int leastLessThanValue = lessThanValues.first();
				if (equalValue >= leastLessThanValue) {
					return;
				}
			} else if (hasGreaterThanValues) {
				int greatestGreaterThanValue = greaterThanValues.last();
				if (equalValue <= greatestGreaterThanValue) {
					return;
				}
			}
			columnPredDataList.add(new int[]{0, equalValue});
			return;

		} else if (equalValues.size() == 0) { // 0 equal signs
			if (hasLessThanValues && hasGreaterThanValues) {
				int leastLessThanValue = lessThanValues.first();
				int greatestGreaterThanValue = greaterThanValues.last();
				if (leastLessThanValue > greatestGreaterThanValue) {
					columnPredDataList.add(new int[]{1, leastLessThanValue});
					columnPredDataList.add(new int[]{2, greatestGreaterThanValue});
				} 
			} else if (hasLessThanValues) {
				int leastLessThanValue = lessThanValues.first();
				columnPredDataList.add(new int[]{1, leastLessThanValue});
			} else if (hasGreaterThanValues) {
				int greatestGreaterThanValue = greaterThanValues.last();
				columnPredDataList.add(new int[]{2, greatestGreaterThanValue});
			}
			return;
		}

	}
	
	public String estimatePredicates(String fromData, HashMap<Character, ArrayList<int[]>> getTablePredicateMap) {
		// for each table that has an and predicate, update Catalog with the estimate
		HashSet<Character> andDataSet = new HashSet<>(getTablePredicateMap.keySet());
		Iterator<Character> andDataSetItr = andDataSet.iterator();
		
		// Goes through all the tables with predicates
		while (andDataSetItr.hasNext()) { // iterates through the table names
			char tableName = andDataSetItr.next(); // tableName
			
			// String header = Catalog.getHeader(tableName + ".dat");
			
			ArrayList<int[]> predicateDataList = getTablePredicateMap.get(tableName);
			// condense predicates
			// int[]
			// index 0 - column
			// index 1 - operator
			// index 2 - compare value
			
			// int[] operatorCounter = new int[3];
			// index 0 - =
			// index 1 - <
			// index 2 - >
			
			Iterator<int[]> predicateDataItr = predicateDataList.iterator();

			HashMap<Integer, ArrayList<int[]>> columnToPredicate = new HashMap<>();

			while (predicateDataItr.hasNext()) {
				addPredicateData(columnToPredicate, predicateDataItr.next());
			}

			reducePredicates(columnToPredicate); // reduces the predicates for each column to 2 or less predicates

			


			// Calculate the estimated number of rows after the predicates
			int tableNumOfRows = Catalog.getRows(tableName + ".dat");
			// System.out.println("Original NumOfRows is " + tableNumOfRows);
			double estimateRows = tableNumOfRows;

			HashSet<Integer> columnsPredicateKeySet = new HashSet<>(columnToPredicate.keySet());
			Iterator<Integer> columnsPredicateKeySetItr = columnsPredicateKeySet.iterator();
			while (columnsPredicateKeySetItr.hasNext()) {
				int column = columnsPredicateKeySetItr.next();
				int min = Catalog.getMin(tableName + ".dat", column);
				int max = Catalog.getMax(tableName + ".dat", column);
				int uniqueColValues = Catalog.getUnique(tableName + ".dat", column);
				ArrayList<int[]> predDataList = columnToPredicate.get(column);
				for (int[] predData : predDataList) {
					int operator = predData[0];
					int compareValue = predData[1];
					
					double currPredEstimate = estimatePredicateRows(tableNumOfRows, min, max, uniqueColValues, operator, compareValue);
					// System.out.println("estimate of 1st sigma is " + currPredEstimate);
					estimateRows *= (double) currPredEstimate / tableNumOfRows;
				}
			}
			int intEstimateRows = (int) Math.round(estimateRows);
			///
			// System.out.println("tableName is " + tableName);
			// System.out.println("tableNumOfRows is " + tableNumOfRows);
			// System.out.println("intEstimateRows is " + intEstimateRows);
			///
			int removedRows =  tableNumOfRows - intEstimateRows;

			int[] uniqueCols = Catalog.getUniqueColumns(tableName + ".dat");
			uniqueCols = Arrays.copyOf(uniqueCols, uniqueCols.length);
			for (int i = 0; i < uniqueCols.length; i++) { // removes the amount of of removed rows from the number of unique values for each column
				int newValue = uniqueCols[i] - removedRows;
				uniqueCols[i] = (newValue <= 0) ? 1 : newValue; // makes sure the number of unique values is at least 1
			}

			TableMetaData tmd = new TableMetaData(Catalog.getHeader(tableName + ".dat"));
			tmd.setRows(intEstimateRows);
			tmd.setUnique(uniqueCols);
			tmd.setColumns(Catalog.getColumns(tableName + ".dat"));

			Catalog.addData(Character.toLowerCase(tableName) + ".dat", tmd);
			
			// and lowercase the table name 
			fromData = fromData.replace(tableName, Character.toLowerCase(tableName));
		}
		
		return fromData;
	}
	
	public double estimatePredicateRows(double tableNumRows, int min, int max, int uniqueColValues, int operator, int compareValue) {
		switch (operator) {
			case 0: // equals
				return (double) tableNumRows / uniqueColValues;
			case 2: // greater than
				return (double) tableNumRows * (max - compareValue) / (max - min);
			case 1: // less than
				return (double) tableNumRows * (compareValue - min) / (max - min);
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
		// System.out.println(bestOrderMap);
	}

	
	// old selingers
	public String selingersAlgorithm(String fromData, AndData andData) {
		// System.out.println("fromData is " + fromData);
		HashMap<Character, ArrayList<int[]>> tablePredicateMap = andData.getTablePredicateMap();
		// Calculates new data for tables with predicates
		// and switches values to lowercase in fromData

		// Commenting out estimatePredicate because we don't need it /////////////////////////////////////////////////////////////////////////
		//String newFromData = estimatePredicates(fromData, tablePredicateMap);

		// Loads pairs of 2 into the costMap (passes in both orders)
		// loadInPairs(newFromData); // loads in the all capital version ///////////////////////////////////////////////////////
		loadInPairs(fromData); // loads in the all capital version
		
		// Selinger's Algorithm for the best join ordering
		// System.out.println("fromData: " + newFromData);

		// String bestOrder = computeBest(newFromData); /////////////////////////////////////////////////////////////////
		String bestOrder = computeBest(fromData);

		String c1 = bestOrder.substring(0, 1);
		String c2 = bestOrder.substring(1, 2);

		double cost1 = estimateCost(c1);
		double cost2 = estimateCost(c2);

		// makes sure the smaller table is first
		if (cost2 < cost1) {
			return c2 + "" + c1 + bestOrder.substring(2);
		} else {
			return bestOrder;
		}

		// OLD - made sure the second table wasn't a predicate
		// if (Character.isUpperCase(c1) && Character.isLowerCase(c2)) { // first table has a predicate - we are fine
		// 	// switch order of the first two tables - predicate is first
		// 	String subString = bestOrder.substring(2);
		// 	return c2 + "" + c1 + subString;
		// } else {
		// 	return bestOrder;
		// }
		
		
	}
	
	public String computeBest(String rels) {
		// System.out.println("rels is " + rels);
		if (bestOrderMap.containsKey(rels))
			return bestOrderMap.get(rels);
		
		double bestCost = Double.MAX_VALUE;
		String bestOrder = rels;
		
		for (char c : rels.toCharArray()) {
			// System.out.println("The char c is " + c);
			String internalOrder = computeBest(rels.replace(Character.toString(c), ""));
			// System.out.println("internalOrder is " + internalOrder);
			double cost1 = cost(c + internalOrder);
			double cost2 = cost(internalOrder + c);

			// System.out.println("cost for " + (c + internalOrder) + " is " + cost1);
			// System.out.println("cost for " + (internalOrder + c) + " is " + cost2);

			String internalBestOrder = "";
			double internalBestCost = -1;
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
		
		// System.out.println("Last rels is " + rels);
		bestOrderMap.put(rels, bestOrder);
		// System.out.println("bestCost is " + bestCost);
		return bestOrder;
	}

	public double estimateCost(String tables) {
		///
		// String testTable = "BA";
		///

		double product = 1;

		HashMap<Character, Double> predicateRowsMap = new HashMap<>();
		
		HashSet<Character> joinedTables = new HashSet<>();
		// join another table - add the product to totalSum
		for (int i = 0; i < tables.length(); i++) {
			char currTable = tables.charAt(i);
			joinedTables.add(currTable); // adds the table to the set of joined tables

			// check for predicate
			String tableName = currTable + ".dat";
			double tableNumRows = Catalog.getRows(tableName);

			// System.out.println("product before row multiplication is " + product);
			product *= tableNumRows;
			// System.out.println("product after row multiplication is " + product);
			predicateRowsMap.put(currTable, tableNumRows);


			if (tablePredicateMap.containsKey(currTable)) { // predicate map - tablePredicateMap
				ArrayList<int[]> predDataList = tablePredicateMap.get(currTable);
				for (int[] predData : predDataList) {
					int column = predData[3];

					// System.out.println("column is " + column);
					// System.out.println("min is " + Catalog.getMin(tableName, column));
					// System.out.println("max is " + Catalog.getMax(tableName, column));
					// System.out.println("operator is " + predData[1]);
					// System.out.println("compareValue is " + predData[2]);
					// System.out.println("estimatePredicateRows is " + estimatePredicateRows(tableNumRows, Catalog.getMin(tableName, column), Catalog.getMax(tableName, column), Catalog.getUnique(tableName, column), predData[1], predData[2]));
					// System.out.println("tableNumRows is " + tableNumRows);
					// System.out.println("before upper product is " + product);
					// System.out.println("to multiply to product is " + (estimatePredicateRows(tableNumRows, Catalog.getMin(tableName, column), Catalog.getMax(tableName, column), Catalog.getUnique(tableName, column), predData[1], predData[2]) / tableNumRows));
					double predicateEstimateRows = estimatePredicateRows(tableNumRows, Catalog.getMin(tableName, column), Catalog.getMax(tableName, column), Catalog.getUnique(tableName, column), predData[1], predData[2]) / tableNumRows;
					predicateRowsMap.put(currTable, predicateRowsMap.get(currTable) * predicateEstimateRows);
					// System.out.println("per is " + predicateEstimateRows);
					// System.out.println("upper product before is " + product);
					product *= (double) predicateEstimateRows;
					// System.out.println("upper product after is " + product);
				}
			}

			// multiplies by the number of rows in the table
			

			// check for joins if we're not at the first table
			if (i > 0) {
				boolean hasJoinPredicate = false;
				// System.out.println("getting here");
				// System.out.println("whereTables size is " + whereTables.size());
				ArrayList<HashMap<Character, String>> tempTempWhereTables = new ArrayList<>(this.tempWhereTables);
				Iterator<HashMap<Character, String>> tempWhereTablesItr = tempTempWhereTables.iterator();
				while (tempWhereTablesItr.hasNext()) { // iterates through all the joins
					HashMap<Character, String> joinMap = tempWhereTablesItr.next();
					char[] joinTables = new char[2];
					int[] joinColumns = new int[2];
					int tableNumber = 0;
					boolean foundMap = false;

					if (joinMap.keySet().contains(currTable)) { // if the current join has the current table
						foundMap = true;
						for (char c : joinMap.keySet()) {
							if (!joinedTables.contains(c)) {
								foundMap = false;
								break;
							}
							joinTables[tableNumber] = c;
							String tableAndColumn = joinMap.get(c);
							joinColumns[tableNumber] = Integer.parseInt(tableAndColumn, 3, tableAndColumn.length(), 10);
							tableNumber++;
						}
					}
					

					if (foundMap) {
						// System.out.println("found map");
						hasJoinPredicate = true;
						// check which columns are keys
						long table1NumRows = Math.round(predicateRowsMap.get(joinTables[0]));
						long table2NumRows = Math.round(predicateRowsMap.get(joinTables[1]));

						long table1OriginalRows = Catalog.getRows(joinTables[0] + ".dat");
						long table2OriginalRows = Catalog.getRows(joinTables[1] + ".dat");

						double perc1 = (double) table1NumRows / table1OriginalRows;
						double perc2 = (double) table2NumRows / table2OriginalRows;

						long table1NumUniqueCol = Catalog.getUnique(joinTables[0] + ".dat", joinColumns[0]);
						long table2NumUniqueCol = Catalog.getUnique(joinTables[1] + ".dat", joinColumns[1]);

						boolean table1IsKey = (table1NumUniqueCol == table1OriginalRows) || (table1NumUniqueCol == table1OriginalRows - 1);
						boolean table2IsKey = (table2NumUniqueCol == table2OriginalRows) || (table2NumUniqueCol == table2OriginalRows - 1);

						// table1NumUniqueCol *= perc1;
						// table2NumUniqueCol *= perc2;

						table1NumUniqueCol = (table1NumUniqueCol <= 0) ? 1 : table1NumUniqueCol;
						table2NumUniqueCol = (table2NumUniqueCol <= 0) ? 1 : table2NumUniqueCol;

						long minUnique = table1NumUniqueCol < table2NumUniqueCol ? table1NumUniqueCol : table2NumUniqueCol;

						// if (tables.equals(testTable)) {
						// 	System.out.println("table1 is key is " + table1IsKey);
						// 	System.out.println("table2 is key is " + table2IsKey);
						// }
						

						long maxRows = table1NumRows * table2NumRows;
						long estimateRows = -1;
						// not sure what are keys
						if (table1IsKey && table2IsKey) { // both columns are keys
							estimateRows = (table1NumRows < table2NumRows) ? table1NumRows : table2NumRows;
						} else if (table1IsKey || table2IsKey) { // one table is a key
							estimateRows = (table1NumRows > table2NumRows) ? table1NumRows : table2NumRows;
						} else { // no keys
							estimateRows = table1NumRows * table2NumRows * minUnique / table1NumUniqueCol / table2NumUniqueCol; // updates number of rows
						}

						// if (tables.equals(testTable)) {
						// 	System.out.println("table1NumUniqueCol is " + table1NumUniqueCol);
						// 	System.out.println("table2NumUniqueCol is " + table2NumUniqueCol);
						// 	System.out.println("minUnique is " + minUnique);
						// 	System.out.println("table1NumRows is " + table1NumRows);
						// 	System.out.println("table2NumRows is " + table2NumRows);
						// 	System.out.println("estimateRows is " + (double) estimateRows);
						// 	System.out.println("max rows is " + maxRows);
						// 	System.out.println("non-predicate before product is " + product);
						// 	System.out.println("non-predicate to multiply is " + ((double) estimateRows / maxRows));
						// }
							
						product *= (double) estimateRows / maxRows;
						// if (tables.equals(testTable)) {
						// 	System.out.println("non-predicate after product is " + product);
						// }
						
						tempWhereTablesItr.remove(); // removes joinMap to make searching shorter next time and for one table equijoins at the end

					}
				}
			}
		}


		return product;
	}

	public double cost(String tables) {
		// System.out.println("tables is " + tables);
		double product = 1;
		double totalSum = 0;
		
		HashSet<Character> joinedTables = new HashSet<>();
		// join another table - add the product to totalSum
		for (int i = 0; i < tables.length(); i++) {
			char currTable = tables.charAt(i);
			joinedTables.add(currTable); // adds the table to the set of joined tables

			// check for predicate
			String tableName = currTable + ".dat";
			int tableNumRows = Catalog.getRows(tableName);
			if (tablePredicateMap.containsKey(currTable)) {
				ArrayList<int[]> predDataList = tablePredicateMap.get(currTable);
				for (int[] predData : predDataList) {
					int column = predData[3];

					// System.out.println("min is " + Catalog.getMin(tableName, column));
					// System.out.println("max is " + Catalog.getMax(tableName, column));
					// System.out.println("operator is " + predData[1]);
					// System.out.println("compareValue is " + predData[2]);
					// System.out.println("estimatePredicateRows is " + estimatePredicateRows(tableNumRows, Catalog.getMin(tableName, column), Catalog.getMax(tableName, column), Catalog.getUnique(tableName, column), predData[1], predData[2]));
					// System.out.println("tableNumRows is " + tableNumRows);
					// System.out.println("before upper product is " + product);
					// System.out.println("to multiply to product is " + (estimatePredicateRows(tableNumRows, Catalog.getMin(tableName, column), Catalog.getMax(tableName, column), Catalog.getUnique(tableName, column), predData[1], predData[2]) / tableNumRows));

					product *= estimatePredicateRows(tableNumRows, Catalog.getMin(tableName, column), Catalog.getMax(tableName, column), Catalog.getUnique(tableName, column), predData[1], predData[2]) / tableNumRows;
					// System.out.println("after upper product is " + product);
				}
			}

			// multiplies by the number of rows in the table
			product *= tableNumRows;

			// check for joins if we're not at the first table
			if (i > 0) {
				boolean hasJoinPredicate = false;
				for (HashMap<Character, String> joinMap : whereTables) {
					char[] joinTables = new char[2];
					int[] joinColumns = new int[2];
					int tableNumber = 0;
					boolean foundMap = false;

					if (joinMap.keySet().contains(currTable)) {
						foundMap = true;
						for (char c : joinMap.keySet()) {
							if (!joinedTables.contains(c)) {
								foundMap = false;
								break;
							}
							joinTables[tableNumber] = c;
							String tableAndColumn = joinMap.get(c);
							joinColumns[tableNumber] = Integer.parseInt(tableAndColumn, 3, tableAndColumn.length(), 10);
							tableNumber++;
						}
					}
					

					if (foundMap) {
						hasJoinPredicate = true;
						// check which columns are keys
						long table1NumRows = Catalog.getRows(joinTables[0] + ".dat");
						long table2NumRows = Catalog.getRows(joinTables[1] + ".dat");

						int table1NumUniqueCol = Catalog.getUnique(joinTables[0] + ".dat", joinColumns[0]);
						int table2NumUniqueCol = Catalog.getUnique(joinTables[1] + ".dat", joinColumns[1]);
						
						int minUnique = table1NumUniqueCol < table2NumUniqueCol ? table1NumUniqueCol : table2NumUniqueCol;

						boolean table1IsKey = (table1NumUniqueCol == table1NumRows);
						boolean table2IsKey = (table2NumUniqueCol == table2NumRows);

						long maxRows = table1NumRows * table2NumRows;
						long estimateRows = -1;
						// not sure what are keys
						if (table1IsKey && table2IsKey) { // both columns are keys
							estimateRows = (table1NumRows < table2NumRows) ? table1NumRows : table2NumRows;
						} else if (table1IsKey || table2IsKey) { // one table is a key
							estimateRows = (table1NumRows > table2NumRows) ? table1NumRows : table2NumRows;
						} else { // no keys
							estimateRows = table1NumRows * table2NumRows * minUnique / table1NumUniqueCol / table2NumUniqueCol; // updates number of rows
						}
						// System.out.println("table1NumRows is " + table1NumRows);
						// System.out.println("table2NumRows is " + table2NumRows);
						// System.out.println("estimateRows is " + (float) estimateRows);
						// System.out.println("max rows is " + maxRows);
						// System.out.println("non-predicate before product is " + product);
						// System.out.println("non-predicate to multiply is " + ((float) estimateRows / maxRows));
						product *= (double) estimateRows / maxRows;
						// System.out.println("non-predicate after product is " + product);
					}
				}

				// add calculated value to totalSum
				if (hasJoinPredicate) {
					// System.out.println("product is " + product);
					totalSum += product;
				} else {
					return Float.MAX_VALUE;
				}
				
			}
		}


		return totalSum;
	}
	
	// OLD COST //////////////////////////////////////////////////////////////////////////////////
	// public int cost(String tables) {
	// 	// System.out.println("Calculating costs of " + tables);
	// 	// String tables is just a string, where each character is a table
	// 	char[] tablesCharArray = tables.toCharArray();
	// 	String table1 = Character.toString(tablesCharArray[0]);
		
	// 	String table1FileName = table1 + ".dat";
		
	// 	int table1NumRows = Catalog.getRows(table1FileName);
	// 	int table1NumCols = Catalog.getColumns(table1FileName);

	// 	int[] table1UniqueColumns = Arrays.copyOf(Catalog.getUniqueColumns(table1FileName), table1NumCols);
		
	// 	StringBuilder table1HeaderSB = new StringBuilder(Catalog.getHeader(table1FileName));

	// 	table1 = table1.toUpperCase();
		
	// 	// goes through table string
	// 	for (int i = 1; i < tablesCharArray.length; i++) {
	// 		char table2 = tablesCharArray[i];
	// 		String table2FileName = table2 + ".dat"; // uses this to get metadata
			
	// 		table2 = Character.toUpperCase(table2);
			
	// 		char table1JoinTable = ' ';
	// 		char table2JoinTable = ' ';
	// 		HashMap<Character, String> equijoinMap = new HashMap<>();

	// 		// System.out.println("i is " + i);
	// 		// System.out.println("table1 is " + table1);
	// 		// System.out.println("table2 is " + table2);
			
	// 		// returns the map of the equijoin
	// 		// and the char of the table in table1 that is a part of the join
	// 		boolean found = false;
	// 		for (char c : table1.toCharArray()) {
	// 			char actualTable = c;
	// 			if (possibleJoinsMap.get(actualTable).contains(table2)) {
	// 				table1JoinTable = actualTable;
	// 				table2JoinTable = table2;
	// 				found = true;
	// 			}
	// 		}

	// 		if (!found) return Integer.MAX_VALUE;

	// 		for (HashMap<Character, String> map : whereTables) {
	// 			if (map.containsKey(table1JoinTable) && map.containsKey(table2JoinTable)) {
	// 				equijoinMap = map;
	// 			}
	// 		}
			
			
	// 		// if (equijoinMap == null) { // no equijoin, so cost is high
	// 		// 	return Integer.MAX_VALUE;
	// 		// }
			

			
	// 		// use header to get the number of unique values in the right columns
	// 		String[] table1HeaderArr = table1HeaderSB.toString().split(",");
	// 		String table1JoinColName = equijoinMap.get(table1JoinTable);

	// 		String table2Header = Catalog.getHeader(table2FileName);
	// 		String[] table2HeaderArr = table2Header.split(",");
	// 		String table2JoinColName = equijoinMap.get(table2);
			
	// 		// gets integer join columns
	// 		int table1JoinCol = findIndex(table1HeaderArr, table1JoinColName);
	// 		int table2JoinCol = findIndex(table2HeaderArr, table2JoinColName);
			
	// 		int[] table2UniqueColumns = Arrays.copyOf(Catalog.getUniqueColumns(table2FileName), table2HeaderArr.length);
			
	// 		int table1NumUniqueCol = table1UniqueColumns[table1JoinCol];
	// 		int table2NumUniqueCol = Catalog.getUnique(table2FileName, table2JoinCol);
			
	// 		int minUnique = table1NumUniqueCol < table2NumUniqueCol ? table1NumUniqueCol : table2NumUniqueCol;
			
	// 		// calculates meta data for current table1
	// 		int table2NumRows = Catalog.getRows(table2FileName);

	// 		// System.out.println("table1 is " + table1);
	// 		// System.out.println("table2 is " + table2);

	// 		// System.out.println("table1JoinCol is " + table1JoinCol);
	// 		// System.out.println("table2JoinCol is " + table2JoinCol);

	// 		// System.out.println("table1NumRows is " + table1NumRows);
	// 		// System.out.println("table2NumRows is " + table2NumRows);

	// 		// System.out.println("table1NumUniqueCol is " + table1NumUniqueCol);
	// 		// System.out.println("table2NumUniqueCol is " + table2NumUniqueCol);
	// 		// System.out.println("minUnique is " + minUnique);

	// 		int maxRows = table1NumRows * table2NumRows;
	// 		// estimated rows for joined table

	// 		boolean table1IsKey = (table1NumUniqueCol == table1NumRows);
	// 		boolean table2IsKey = (table2NumUniqueCol == table2NumRows);


	// 		// not sure what are keys
	// 		if (table1IsKey && table2IsKey) { // both columns are keys
	// 			table1NumRows = (table1NumRows < table2NumRows) ? table1NumRows : table2NumRows;
	// 		} else if (table1IsKey || table2IsKey) { // one table is a key
	// 			table1NumRows = (table1NumRows > table2NumRows) ? table1NumRows : table2NumRows;
	// 		} else { // no keys
	// 			table1NumRows = table1NumRows * table2NumRows * minUnique / table1NumUniqueCol / table2NumUniqueCol; // updates number of rows
	// 		}

	// 		// just no keys for now
	// 		// table1NumRows = table1NumRows * table2NumRows * minUnique / table1NumUniqueCol / table2NumUniqueCol; // updates number of rows
			
	// 		// System.out.println("New Estimate is " + table1NumRows);
			
			
	// 		if (table1HeaderSB.toString().indexOf(table2JoinCol) < 0) { // table2 join col not is already in table1 header
	// 			table1HeaderSB.append("," + table2Header); // updates table1 header
				
	// 			table1UniqueColumns = combineRows(table1UniqueColumns, table2UniqueColumns);// updates unique columns
	// 		}

	// 		if (table1IsKey && table2IsKey) { // if both are keys, set the unique values to the min
	// 			for (int j = 0; j < table1UniqueColumns.length; j++) {
	// 				table1UniqueColumns[j] = (table1UniqueColumns[j] > table1NumRows) ? table1NumRows : table1UniqueColumns[j];
	// 			}

	// 		} else if (table1IsKey || table2IsKey) {
	// 			// no change for distinct values?
	// 		} else { // neither column is a key
	// 			// percentage to decrease number of unique values
	// 			double percentage = (double) table1NumRows / maxRows;

	// 			for (int index = 0; index < table1UniqueColumns.length; index++) {
	// 				int value = (int) Math.round(table1UniqueColumns[index] * percentage);
	// 				if (value == 0) { // makes sure no value is 0 so we don't divide by 0
	// 					table1UniqueColumns[index] = 1;
	// 				} else {
	// 					table1UniqueColumns[index] = value;
	// 				}
	// 			}
	// 		}
			
			
			
	// 		// updates table1 name
	// 		table1 += table2; 
			
	// 	}
		
	// 	return table1NumRows;
	// }
	// OLD COST //////////////////////////////////////////////////////////////////////////////////
	
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

	public HashMap<Character, String> getJoinMap(char table1, char table2) {
		for (HashMap<Character, String> joinMap : whereTables) {
			if (joinMap.containsKey(table1) && joinMap.containsKey(table2)) {
				return joinMap;
			}
		}
		return null; // shouldn't get here
	}
	
	
	public void optimizeQuery(String columnNames, String fromData, WhereData whereData, AndData andData) throws FileNotFoundException {

		bestOrderMap = new HashMap<>();

		tablePredicateMap = andData.getTablePredicateMap();
		
		

		HashMap<Character, TreeSet<Integer>> tableToColumnsToKeep = new HashMap<>();
		String[] columnNamesArr = columnNames.split(",");
		for (String columnName : columnNamesArr) { // adds the select columns
			char c = columnName.charAt(0);
			int joinCol = Integer.parseInt(columnName.substring(3));

			TreeSet<Integer> tempList = new TreeSet<>();
			if (tableToColumnsToKeep.containsKey(c)) {
				tempList = tableToColumnsToKeep.get(c);
			}
			tempList.add(joinCol);
			// tempList = null;
			tableToColumnsToKeep.put(c, tempList);
		}

		whereTables = whereData.getTables();
		tempWhereTables = new ArrayList<>(whereTables);
		// add whereData columns
		for (HashMap<Character, String> joinMap : whereTables) {
			for (char c : joinMap.keySet()) {
				int joinCol = Integer.parseInt(joinMap.get(c).substring(3));
				
				TreeSet<Integer> tempList = new TreeSet<>();
				if (tableToColumnsToKeep.containsKey(c)) {
					tempList = tableToColumnsToKeep.get(c);
				}
				tempList.add(joinCol);
				// tempList = null;
				tableToColumnsToKeep.put(c, tempList);
			}
		}

		// adds andData columns
		for (char c : tablePredicateMap.keySet()) {
			ArrayList<int[]> predicateDataList = tablePredicateMap.get(c);
			TreeSet<Integer> tempList = new TreeSet<>();
			if (tableToColumnsToKeep.containsKey(c)) {
				tempList = tableToColumnsToKeep.get(c);
			}

			for (int[] predData : predicateDataList) {
				tempList.add(predData[0]); // column number
			}
			tableToColumnsToKeep.put(c, tempList); // char -> TreeSet
		}

		
		possibleJoinsMap = new HashMap<>(); // maps tables to the tables they join with
		HashSet<Character> allTables = new HashSet<>(); // list of all the tables

		
		// iterates through the joins and makes the possibleJoinsMap
		for (int i = 0; i < whereTables.size(); i++) {
			HashMap<Character, String> joinMap = whereTables.get(i);
			HashSet<Character> tableNames = new HashSet<>(joinMap.keySet());
			Iterator<Character> tableNamesItr = tableNames.iterator();
			// only two tables
			char table1 = tableNamesItr.next();
			char table2 = tableNamesItr.next();

			// list of all of the tables (characters)
			allTables.add(table1);
			allTables.add(table2);

			// updates table 1 possible joins
			HashSet<Character> possibleJoins = new HashSet<>();
			if (possibleJoinsMap.containsKey(table1)) {
				possibleJoins = possibleJoinsMap.get(table1);
			}
			possibleJoins.add(table2);
			possibleJoinsMap.put(table1, possibleJoins);

			// updates table 2 possible joins
			possibleJoins = new HashSet<>();
			if (possibleJoinsMap.containsKey(table2)) {
				possibleJoins = possibleJoinsMap.get(table2);
			}
			possibleJoins.add(table1);
			possibleJoinsMap.put(table2, possibleJoins);
		}

		Queue<HashSet<Character>> leftDeepJoins = new LinkedList<>();

		// while allTables is not empty, make left deep joins
		while (!allTables.isEmpty()) {
			Iterator<Character> allTablesItr = allTables.iterator();
			char firstTable = allTablesItr.next();
			allTablesItr.remove(); // removes the table from the list of all tables
			// makes a queue of tables to look for joins
			Queue<Character> tablesToLook = new LinkedList<>();
			tablesToLook.add(firstTable);
			// set of the left deep join
			HashSet<Character> leftDeepJoin = new HashSet<>();
			leftDeepJoin.add(firstTable);
			while (!tablesToLook.isEmpty()) {
				char currTable = tablesToLook.remove(); // current table to check joinings
				HashSet<Character> currTableJoins = possibleJoinsMap.get(currTable);

				Iterator<Character> currTableJoinsItr = currTableJoins.iterator();
				while (currTableJoinsItr.hasNext()) {
					char currJoin = currTableJoinsItr.next();
					// join table is not already added
					if (leftDeepJoin.add(currJoin)) {
						tablesToLook.add(currJoin); // need to see what the new table can join to
						allTables.remove(currJoin); // removes the joining from the set of all tables
					} 
				}
			}
			leftDeepJoins.add(leftDeepJoin);
		}

		// find optimal join order
		Queue<String> bestOrders = new LinkedList<>();

		// for each left deep join
		while (!leftDeepJoins.isEmpty()) {
			HashSet<Character> leftDeepJoin = leftDeepJoins.remove();

			// build string of all tables
			StringBuilder sb = new StringBuilder();
			Iterator<Character> leftDeepJoinItr = leftDeepJoin.iterator();
			while (leftDeepJoinItr.hasNext()) {
				sb.append(leftDeepJoinItr.next());
			}

			String bestOrder = selingersAlgorithm(sb.toString(), andData);
			bestOrders.add(bestOrder);
		}


		// HashMap<Character, Integer> totalStartingIndexMap = new HashMap<>();
		// int totalStartingIndex = 0;

		// Overall Header for the join columns
		StringBuilder overallHeader = new StringBuilder();

		// for each ordering
		while (!bestOrders.isEmpty()) {
			// Header for the join columns
			StringBuilder header = new StringBuilder();

			Queue<RAOperation> tableQueue = new LinkedList<>();
			Queue<Predicate> predicateQueue = new LinkedList<>();
			Queue<Boolean> switchQueue = new LinkedList<>();
			// HashMap from tableName -> starting index of table
			// HashMap<Character, Integer> tableNameToStartingIndexMap = new HashMap<>();

			HashSet<Character> inJoin = new HashSet<>();

			// int startingIndex = 0;

			String bestOrder = bestOrders.remove(); // String in the best order for table joinings

			// switch order if first table is not a predicate and second table is a predicate
			// if (!tablePredicateMap.containsKey(bestOrder.charAt(0)) && tablePredicateMap.containsKey(bestOrder.charAt(1))) {
			// 	bestOrder = bestOrder.charAt(1) + "" + bestOrder.charAt(0) + bestOrder.substring(2);
			// }

			// checks the predicate to see if they will return any results
			for (char c : tablePredicateMap.keySet()) {
				ArrayList<int[]> predDataList = tablePredicateMap.get(c);
				for (int[] predData : predDataList) {
					int column = predData[3];
					int operator = predData[1];
					int compareValue = predData[2];
					if (operator == 0) { // only checks equals
						int min = Catalog.getMin(c + ".dat", column);
						int max = Catalog.getMax(c + ".dat", column);
						if (compareValue < min || compareValue > max) {
							this.columnsToSum = new int[columnNamesArr.length];
							tableQueue.add(new Scan("Empty.dat", this.columnsToSum));
							predicateQueue.add(new PlacePredicate());
							tablesQueue.add(tableQueue);
							predicatesQueue.add(predicateQueue);
							switchesQueue.add(switchQueue);
							return;
						}
					}
				}
			}

			System.err.println("Best Order is " + bestOrder); 
			char table1 = bestOrder.charAt(0);

			// columns to keep
			TreeSet<Integer> columnsToKeepSet = new TreeSet<>(tableToColumnsToKeep.get(Character.toUpperCase(table1)));
			int[] columnsToKeep = new int[columnsToKeepSet.size()];

			int columnsToKeepIndex = 0;
			// make columnsToKeep and header
			while (!columnsToKeepSet.isEmpty()) {
				int col = columnsToKeepSet.first();
				columnsToKeepSet.remove(col);
				columnsToKeep[columnsToKeepIndex] = col;
				header.append(Character.toUpperCase(table1) + ".c" + col + ",");
				columnsToKeepIndex++;
			}

			header.delete(header.length() - 1, header.length()); // get rid of comma
			String[] headerArr = header.toString().split(",");
			header.append(','); // add comma

			char table2PredicateCheck = bestOrder.charAt(1);
			if (tablePredicateMap.containsKey(table1)) {
				// add a predicate
				//table1 = Character.toUpperCase(table1);
				ArrayList<int[]> predDataList = tablePredicateMap.get(table1);
				for (int i = 0; i < predDataList.size(); i++) { // updates predicate columns to the correct columns
					int[] predData = predDataList.get(i);
					predData[0] = findIndex(headerArr, table1 + ".c" + predData[0]);
				}
				predicateQueue.add(new FilterPredicate(predDataList, table1 + ".dat", columnsToKeep));
			} else if (tablePredicateMap.containsKey(table2PredicateCheck)){
				// add placePredicate
				predicateQueue.add(new PlacePredicate());
			}

			// add table1 to inJoin
			inJoin.add(table1);

			// Add table
			//tableQueue.add(new Project(new Scan(table1 + ".dat"), Arrays.copyOf(columnsToKeep, columnsToKeep.length)));
			if (!tablePredicateMap.containsKey(table1)) {
				tableQueue.add(new ProjectScan(table1 + ".dat", Arrays.copyOf(columnsToKeep, columnsToKeep.length)));
			}

			// starting index map
			// int table1NumCols = Catalog.getColumns(table1 + ".dat");
			// tableNameToStartingIndexMap.put(table1, startingIndex);
			// startingIndex += table1NumCols;

			// totalStartingIndexMap.put(table1, totalStartingIndex);
			// totalStartingIndex += table1NumCols;

			// HashSet<Character> alreadyJoinedTables = new HashSet<>();
			// alreadyJoinedTables.add(table1);

			// Start iterating the other tables - start joining
			char[] bestOrderCharArr = bestOrder.toCharArray();
			for (int i = 1; i < bestOrderCharArr.length; i++) {
				

				char table2 = bestOrder.charAt(i);

				boolean doMergeJoin = true;
				// boolean doMergeJoin = false;
				// if (table2Rows > 20000) {
				// 	doMergeJoin = true;
				// }

				StringBuilder header2 = new StringBuilder();
				// gets the tables currently in the join
				String currTable = bestOrder.substring(0, i);
				// estimates the bigger table
				boolean switchTables = false;
				// if (cost(currTable) > Catalog.getRows(table2 + ".dat")) {
				//if (estimateCost(currTable) >= 5000 && Catalog.getRows(table2 + ".dat") < 20000) {
					
				double table1EstimateCost = estimateCost(currTable);
				double table2EstimateCost = estimateCost(Character.toString(table2));
				// System.out.println("currTable is " + currTable);
				// System.out.println("estimate is " + table1EstimateCost);
				// System.out.println("table2 is " + table2);
				// System.out.println("estimate is " + table2EstimateCost);
				int threshold = 30000;
				// OLD ORIGINAL
				// if (table1EstimateCost >= threshold && table2EstimateCost >= threshold) {
				// 	switchTables = false;
				// 	doMergeJoin = true;
				// } else if (table1EstimateCost >= threshold && table2EstimateCost < 2000) {
				// 	switchTables = true;
				// 	doMergeJoin = false;
				// } else {
				// 	if (table2EstimateCost < table1EstimateCost) {
				// 		switchTables = true;
				// 	} else {
				// 		switchTables = false;
				// 	}
				// 	doMergeJoin = false;
				// }

				// BETTER?
				if (table1EstimateCost >= threshold) {
					if (table2EstimateCost < 20000) {
						switchTables = true;
						doMergeJoin = false;
					} else {
						switchTables = false;
						doMergeJoin = true;
					}
				} else {
					if (table2EstimateCost < table1EstimateCost) {
						switchTables = true;
					} else {
						switchTables = false;
					}
					doMergeJoin = false;
				}

				// testing ----------------------
				// switchTables = false;
				// doMergeJoin = false;

				
				columnsToKeepSet = new TreeSet<>(tableToColumnsToKeep.get(Character.toUpperCase(table2)));
				columnsToKeep = new int[columnsToKeepSet.size()];
				columnsToKeepIndex = 0;
				// make columnsToKeep and header
				while (!columnsToKeepSet.isEmpty()) {
					int col = columnsToKeepSet.first();
					columnsToKeepSet.remove(col);
					columnsToKeep[columnsToKeepIndex] = col;
					header2.append(Character.toUpperCase(table2) + ".c" + col + ",");
					columnsToKeepIndex++;
				}
				header2.delete(header2.length() - 1, header2.length()); // get rid of comma
				String[] header2Arr = header2.toString().split(",");


				//if (Character.isLowerCase(table2)) { // predicate
				if (tablePredicateMap.containsKey(table2)) { // predicate
					//table2 = Character.toUpperCase(table2);
					ArrayList<int[]> predDataList = tablePredicateMap.get(table2);
					for (int j = 0; j < predDataList.size(); j++) { // updates predicate columns to the correct columns
						int[] predData = predDataList.get(j);
						predData[0] = findIndex(header2Arr, table2 + ".c" + predData[0]);
					}
					predicateQueue.add(new FilterPredicate(predDataList, table2 + ".dat", Arrays.copyOf(columnsToKeep, columnsToKeep.length))); // add predicate data
				}

				// add scan anyways
				//tableQueue.add(new Scan(table2 + ".dat"));

				HashMap<Character, String> joinMap = new HashMap<>();
				// get join map
				for (char table : inJoin) {
					// System.out.println("table is " + table);
					// System.out.println("table2 is " + table2);
					joinMap = getJoinMap(table, table2);
					if (joinMap != null) {
						break;
					}
				}
				
				whereTables.remove(joinMap); // removes joinMap to make searching shorter next time and for one table equijoins at the end

				HashSet<Character> tables = new HashSet<>(joinMap.keySet());
				Iterator<Character> tablesItr = tables.iterator();
				char firstTable = tablesItr.next();
				char secondTable = tablesItr.next();

				String table1JoinColName = joinMap.get(firstTable);
				String table2JoinColName = joinMap.get(secondTable);
				// int cIndex = table1JoinColName.indexOf("c");
				// get join column integer values
				// int table1JoinCol = Integer.parseInt(table1JoinColName.substring(cIndex + 1, table1JoinColName.length()));
				// cIndex = table2JoinColName.indexOf("c");
				// int table2JoinCol = Integer.parseInt(table2JoinColName.substring(cIndex + 1, table2JoinColName.length()));
				
				header.delete(header.length() - 1, header.length()); // get rid of comma
				headerArr = header.toString().split(",");
				header.append(','); // add comma

				int table1JoinCol = findIndex(headerArr, table1JoinColName);
				int table2JoinCol = findIndex(headerArr, table2JoinColName);

				boolean containsFirstTable = table1JoinCol != -1;
				boolean containsSecondTable = table2JoinCol != -1;

				///
				// System.out.println("table1JoinColName is " + table1JoinColName);
				// System.out.println("table2JoinColName is " + table2JoinColName);

				// System.out.println("table1JoinCol is " + table1JoinCol);
				// System.out.println("table2JoinCol is " + table2JoinCol);

				// System.out.println("header is " + header.toString());
				// System.out.println("header2 is " + header2.toString());
				///

				if (containsFirstTable && containsSecondTable) { // one table equijoin
					// add fake table
					tableQueue.add(new Scan("Fake Table", null));

					switchQueue.add(false);

					// add one table equijoin predicate
					predicateQueue.add(new EquijoinPredicate(table1JoinCol, table2JoinCol, false));
				} else if (containsFirstTable) { // one table is definitely already joined

					// update starting index map
					// int numOfCols = Catalog.getColumns(secondTable + ".dat");
					// tableNameToStartingIndexMap.put(secondTable, startingIndex);
					// startingIndex += numOfCols;

					// totalStartingIndexMap.put(secondTable, totalStartingIndex);
					// totalStartingIndex += numOfCols;

					// secondTable is added to joined
					inJoin.add(secondTable);

					// get join cols for secondTable
					// columns to keep
					columnsToKeepSet = new TreeSet<>(tableToColumnsToKeep.get(secondTable));
					columnsToKeep = new int[columnsToKeepSet.size()];
					columnsToKeepIndex = 0;
					// make columnsToKeep and header
					while (!columnsToKeepSet.isEmpty()) { // columns to keep for project
						int col = columnsToKeepSet.first();
						columnsToKeepSet.remove(col);
						columnsToKeep[columnsToKeepIndex] = col;
						columnsToKeepIndex++;
					}

					

					// add second table
					//tableQueue.add(new Project(new Scan(secondTable + ".dat"), columnsToKeep));
					if (!tablePredicateMap.containsKey(secondTable)) {
						tableQueue.add(new ProjectScan(secondTable + ".dat", Arrays.copyOf(columnsToKeep, columnsToKeep.length)));
					}
					// need to update table 1 join col - table 2 join col is fine
					// table1JoinCol = tableNameToStartingIndexMap.get(firstTable) + table1JoinCol;

					headerArr = header2.toString().split(",");

					// table 2 join col with table 2 headers added
					table2JoinCol = findIndex(headerArr, table2JoinColName);

					///
					// System.out.println("containsFirstTable:");
					// System.out.println("table2JoinCol is " + table2JoinCol);
					// System.out.println("table1JoinCol is " + table1JoinCol);
					///

					if (switchTables) {
						switchQueue.add(true);
						predicateQueue.add(new EquijoinPredicate(table2JoinCol, table1JoinCol, true));
					} else {
						switchQueue.add(false);
						if (doMergeJoin) {
							predicateQueue.add(new MergeJoinPredicate(table1JoinCol, table2JoinCol));
						} else {
							predicateQueue.add(new EquijoinPredicate(table1JoinCol, table2JoinCol, true));
						}
					}

					// adds mergeJoin Predicate
					// REPLACE MERGE JOIN HERE
					// if (doMergeJoin) {
					// 	predicateQueue.add(new MergeJoinPredicate(table1JoinCol, table2JoinCol));
					// } else {
					// 	predicateQueue.add(new EquijoinPredicate(table1JoinCol, table2JoinCol, true));
					// }

				} else if (containsSecondTable) {

					// update starting index map
					// int numOfCols = Catalog.getColumns(firstTable + ".dat");
					// tableNameToStartingIndexMap.put(firstTable, startingIndex);
					// startingIndex += numOfCols;

					// totalStartingIndexMap.put(firstTable, totalStartingIndex);
					// totalStartingIndex += numOfCols;

					// firstTable is added to joined
					inJoin.add(firstTable);

					columnsToKeepSet = new TreeSet<>(tableToColumnsToKeep.get(firstTable));
					columnsToKeep = new int[columnsToKeepSet.size()];
					columnsToKeepIndex = 0;
					// make columnsToKeep and header
					while (!columnsToKeepSet.isEmpty()) { // columns to keep for project
						int col = columnsToKeepSet.first();
						columnsToKeepSet.remove(col);
						columnsToKeep[columnsToKeepIndex] = col;
						columnsToKeepIndex++;
					}

					// add first table
					// tableQueue.add(new Project(new Scan(firstTable + ".dat"), columnsToKeep));
					if (!tablePredicateMap.containsKey(firstTable)) {
						tableQueue.add(new ProjectScan(firstTable + ".dat", Arrays.copyOf(columnsToKeep, columnsToKeep.length)));
					}

					// need to update table 2 join col - table 1 join col is fine
					// table2JoinCol = tableNameToStartingIndexMap.get(secondTable) + table2JoinCol;

					// gets header of table not already joined
					headerArr = header2.toString().split(",");

					///
					// System.out.println("table1JoinColName is " + table1JoinColName);
					// System.out.println("table2JoinColName is " + table2JoinColName);
					// System.out.println("header2 is " + header2.toString());
					///

					// table 2 join col with table 2 headers added
					table1JoinCol = findIndex(headerArr, table1JoinColName);

					///
					// System.out.println("containsSecondTable:");
					// System.out.println("table2JoinCol is " + table2JoinCol);
					// System.out.println("table1JoinCol is " + table1JoinCol);
					///

					if (switchTables) {
						switchQueue.add(true);
						predicateQueue.add(new EquijoinPredicate(table1JoinCol, table2JoinCol, true));
					} else {
						switchQueue.add(false);
						if (doMergeJoin) {
							predicateQueue.add(new MergeJoinPredicate(table2JoinCol, table1JoinCol));
						} else {
							predicateQueue.add(new EquijoinPredicate(table2JoinCol, table1JoinCol, true));
						}
					}

					// table 2 is now table 1
					// REPLACE MERGE JOIN HERE
					// if (doMergeJoin) {
					// 	predicateQueue.add(new MergeJoinPredicate(table2JoinCol, table1JoinCol));
					// } else {
					// 	predicateQueue.add(new EquijoinPredicate(table2JoinCol, table1JoinCol, true));
					// }
					
					
				}

				if (switchTables) { // gets the right order of the header
					header.insert(0, header2.toString() + ",");
				} else { // regular
					header.append(header2); // adds header of second table to first table
				}
				
				// regular
				// header.append(header2); // adds header of second table to first table
				
				
				// System.out.println(header.toString());

				// look for 1 table equijoins
				Iterator<HashMap<Character, String>> whereTablesItr = whereTables.iterator();
				while (whereTablesItr.hasNext()) {
					HashMap<Character, String> joinMap2 = whereTablesItr.next();
					Iterator<Character> joinMap2Itr = joinMap2.keySet().iterator();
					char joinTable1 = joinMap2Itr.next();
					char joinTable2 = joinMap2Itr.next();

					if (inJoin.contains(joinTable1) && inJoin.contains(joinTable2)) {
						String table1JoinColNameTemp = joinMap2.get(joinTable1);
						String table2JoinColNameTemp = joinMap2.get(joinTable2);

						int table1JoinColTemp = findIndex(header.toString().split(","), table1JoinColNameTemp);
						int table2JoinColTemp = findIndex(header.toString().split(","), table2JoinColNameTemp);


						// add false to switchQueue
						switchQueue.add(false);

						// make 1 table equijoin
						tableQueue.add(new Scan("Fake Table", null));
						predicateQueue.add(new EquijoinPredicate(table1JoinColTemp, table2JoinColTemp, false));
						whereTablesItr.remove();
					}
				}

				if (!switchTables) {
					// adds the comma to header
					header.append(',');
				}

				// regular
				// header.append(',');

				
			}

			// COMMENTED OUT BECAUSE I THINK WE DON'T NEED IT
			// One table equijoins - tables already in joins
			// for (HashMap<Character, String> joinMap : whereTables) {
			// 	HashSet<Character> joinTables = new HashSet<>(joinMap.keySet());
			// 	Iterator<Character> joinTablesItr = joinTables.iterator();
			// 	char joinTable1 = joinTablesItr.next();
			// 	char joinTable2 = joinTablesItr.next();

			// 	if (inJoin.contains(joinTable1) && inJoin.contains(joinTable2)) {
			// 		// do 1 table equijoin
			// 		tableQueue.add(new Scan("Fake Table"));

			// 		String table1JoinColName = joinMap.get(joinTable1);
			// 		String table2JoinColName = joinMap.get(joinTable2);

			// 		header.delete(header.length() - 1, header.length()); // get rid of comma
			// 		headerArr = header.toString().split(",");
			// 		header.append(','); // add comma

			// 		int table1JoinCol = findIndex(headerArr, table1JoinColName);
			// 		int table2JoinCol = findIndex(headerArr, table2JoinColName);

			// 		// table1JoinCol += tableNameToStartingIndexMap.get(joinTable1);
			// 		// table2JoinCol += tableNameToStartingIndexMap.get(joinTable2);

			// 		predicateQueue.add(new EquijoinPredicate(table1JoinCol, table2JoinCol, false));
			// 	}
			// }


			// System.out.println("inner map is: ");
			// System.out.println(totalStartingIndexMap);

			// System.out.println("tableQueue is ");
			// System.out.println(tableQueue);

			// System.out.println("predicateQueue is ");
			// System.out.println(predicateQueue);

			overallHeader.append(header);
			// System.out.println(overallHeader.toString());

			// adds the table queue and predicate queue
			this.switchesQueue.add(new LinkedList<>(switchQueue));
			this.tablesQueue.add(new LinkedList<>(tableQueue));
			this.predicatesQueue.add(new LinkedList<>(predicateQueue));
		}

		// Add finalPredicateQueue
		for (int i = 0; i < predicatesQueue.size() - 1; i++) {
			finalPredicateQueue.add(new EquijoinPredicate(-3, -3, true)); // adds cross products for the disjoint join
		}

		// Add columnsToSum
		// String[] columnNamesArr = columnNames.split(",");
		this.columnsToSum = new int[columnNamesArr.length];

		overallHeader.delete(overallHeader.length() - 1, overallHeader.length());
		
		for (int i = 0; i < columnNamesArr.length; i++) {
			String tableAndColumn = columnNamesArr[i];
			this.columnsToSum[i] = findIndex(overallHeader.toString().split(","), tableAndColumn);
		}

		
	}

}