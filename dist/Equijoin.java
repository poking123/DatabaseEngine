import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.List;

public class Equijoin extends RAOperation implements Iterable<List<int[]>> {
	private Iterable<List<int[]>> source1;
	private Iterable<List<int[]>> source2;
	private String type;
	
	private EquijoinPredicate equijoinPredicate;

	public Equijoin(RAOperation source1, RAOperation source2, EquijoinPredicate equijoinPredicate) {
		this.source1 = source1;
		this.source2 = source2;
		this.equijoinPredicate = equijoinPredicate;
		this.type = "equijoin";
	}

	public Equijoin(RAOperation source1, RAOperation source2, EquijoinPredicate equijoinPredicate, String type) {
		this.source1 = source1;
		this.source2 = source2;
		this.equijoinPredicate = equijoinPredicate;
		this.type = "disjointEquijoin";
	}
	
	String getType() {
		return this.type;
	}
	
	@Override
	public Iterator<List<int[]>> iterator() {
		return new EquijoinIterator(source1.iterator(), source2, equijoinPredicate.isTwoTableJoin(), equijoinPredicate.getTable1JoinCol(), equijoinPredicate.getTable2JoinCol());
	}

	public class EquijoinIterator implements Iterator<List<int[]>> {
		private Iterator<List<int[]>> source1Iterator;
		private Iterable<List<int[]>> source2;
		private boolean isTwoTableJoin;
		private int table1JoinCol;
		private int table2JoinCol;
		
		public EquijoinIterator(Iterator<List<int[]>> source1Iterator, Iterable<List<int[]>> source2, boolean isTwoTableJoin, int table1JoinCol, int table2JoinCol) {
			this.source1Iterator = source1Iterator;
			this.source2 = source2;
			this.isTwoTableJoin = isTwoTableJoin;
			
			this.table1JoinCol = table1JoinCol;
			this.table2JoinCol = table2JoinCol;
		}
		
		@Override
		public boolean hasNext() {
			return source1Iterator.hasNext();
		}

		@Override
		public List<int[]> next() {
			List<int[]> input = source1Iterator.next();
			
				
			List<int[]> rowsToReturn = new ArrayList<>();
			
			if (input.isEmpty()) {
				return rowsToReturn;
			} else {
				// BIG IF - Two table equijoin (tables are merged)
				if (this.isTwoTableJoin) {
					// Sort BNLJ
					TreeMap<Integer, List<Integer>> valueToIndex = new TreeMap<>();
					for (int i = 0; i < input.size(); i++) {
						List<Integer> listOfIndices = new ArrayList<>();
						int value = input.get(i)[this.table1JoinCol];
						if (valueToIndex.containsKey(value)) {
							listOfIndices = valueToIndex.get(value);
						}
						listOfIndices.add(i);
						valueToIndex.put(value, listOfIndices);
					}


					// just have to loop through table 2
					Iterator<List<int[]>> table2RowBlocks = source2.iterator();
					while (table2RowBlocks.hasNext()) {
						List<int[]> table2RowBlock = table2RowBlocks.next();
						for (int[] table2Row : table2RowBlock) {
							int value = table2Row[this.table2JoinCol];
							if (valueToIndex.containsKey(value)) {
								List<Integer> table1MatchingIndices = valueToIndex.get(value);
								
								for (int index : table1MatchingIndices) {
									int[] table1MatchingRow = input.get(index);
									rowsToReturn.add(combineRows(table1MatchingRow, table2Row));
								}
							}
							// else, there are no matches

						}
					}




					// Make HashMap for Hash BNLJ
					// table1JoinCol Value -> index in buffer
					// HashMap<Integer, List<Integer>> bufferMap = new HashMap<>();
					// for (int i = 0; i < input.size(); i++) {
					// 	List<Integer> listOfIndices = new ArrayList<>();

					// 	int[] table1Row = input.get(i);
					// 	int value = table1Row[this.table1JoinCol];
						
					// 	if (bufferMap.containsKey(value)) {
					// 		listOfIndices = bufferMap.get(value);
					// 	}
					// 	listOfIndices.add(i);
					// 	bufferMap.put(value, listOfIndices);
					// }
					
					// // just have to loop through table 2
					// Iterator<List<int[]>> table2RowBlocks = source2.iterator();
					// while (table2RowBlocks.hasNext()) {
					// 	List<int[]> table2RowBlock = table2RowBlocks.next();
					// 	for (int[] table2Row : table2RowBlock) {
					// 		int value = table2Row[this.table2JoinCol];
					// 		if (bufferMap.containsKey(value)) {
					// 			List<Integer> table1MatchingIndices = bufferMap.get(value);
								
					// 			for (int index : table1MatchingIndices) {
					// 				int[] table1MatchingRow = input.get(index);
					// 				rowsToReturn.add(combineRows(table1MatchingRow, table2Row));
					// 			}
					// 		}
					// 		// else, there are no matches

					// 	}
					// }
					
					
//					for (int[] table1Row : input) {
//						Iterator<List<int[]>> table2RowBlocks = source2.iterator();
//						while (table2RowBlocks.hasNext()) {
//							List<int[]> table2RowBlock = table2RowBlocks.next();
//							for (int[] table2Row : table2RowBlock) {
//								if (equijoinPredicate.test(table1Row, table2Row)) {
//									rowsToReturn.add(combineRows(table1Row, table2Row));
//								}	
//							}
//						}
//					}
					
					
					// PRINT - FOR DEBUGGING
					// for (int[] row : rowsToReturn) {
					// 	for (int i : row) {
					// 		System.out.print(i + " ");
					// 	}
					// 	System.out.println();
						
					// }
					// System.out.println();
					return rowsToReturn;
				} else { // is one table join - we only scan table 1 because table 2's headers are already in table 1
					for (int[] table1Row : input) {
						if (equijoinPredicate.test(table1Row)) {
							rowsToReturn.add(table1Row);
						}
					}
					return rowsToReturn;
				}
				
			}
			
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
		
	}

	
	
	
	
	
	
	
}
