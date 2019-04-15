import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Equijoin extends RAOperation {
	private Iterable<Queue<int[]>> source1;
	private Iterable<Queue<int[]>> source2;
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
	public Iterator<Queue<int[]>> iterator() {
		return new EquijoinIterator(source1.iterator(), source2, equijoinPredicate.isTwoTableJoin(), equijoinPredicate.getTable1JoinCol(), equijoinPredicate.getTable2JoinCol());
	}

	public class EquijoinIterator implements Iterator<Queue<int[]>> {
		private Iterator<Queue<int[]>> source1Iterator;
		private Iterable<Queue<int[]>> source2;
		private boolean isTwoTableJoin;
		private int table1JoinCol;
		private int table2JoinCol;
		
		public EquijoinIterator(Iterator<Queue<int[]>> source1Iterator, Iterable<Queue<int[]>> source2, boolean isTwoTableJoin, int table1JoinCol, int table2JoinCol) {
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
		public Queue<int[]> next() {
			Queue<int[]> input = source1Iterator.next();
			
				
			Queue<int[]> rowsToReturn = new LinkedList<>();
			
			if (input.isEmpty()) {
				return rowsToReturn;
			} else {
				// BIG IF - Two table equijoin (tables are merged)
				if (this.isTwoTableJoin) {
					// Sort BNLJ ////////////////////////////
					// TreeMap<Integer, List<int[]>> valueToRows = new TreeMap<>();
					// while (!input.isEmpty()) {
					// 	List<int[]> listOfRows = new LinkedList<>();
					// 	int[] table1Row = input.remove();
					// 	int value = table1Row[this.table1JoinCol];
					// 	if (valueToRows.containsKey(value)) {
					// 		listOfRows = valueToRows.get(value);
					// 	}
					// 	listOfRows.add(table1Row);
					// 	valueToRows.put(value, listOfRows);
					// }

					// // just have to loop through table 2
					// Iterator<Queue<int[]>> table2RowBlocks = source2.iterator();
					// while (table2RowBlocks.hasNext()) {
					// 	Queue<int[]> table2RowsBlock = table2RowBlocks.next();
					// 	while (!table2RowsBlock.isEmpty()) {
					// 		int[] table2Row = table2RowsBlock.remove();
					// 		int value = table2Row[this.table2JoinCol];
					// 		if (valueToRows.containsKey(value)) {
					// 			List<int[]> table1MatchingRows = valueToRows.get(value);
					// 			for (int[] table1MatchingRow : table1MatchingRows) {
					// 				rowsToReturn.add(combineRows(table1MatchingRow, table2Row));
					// 			}
					// 		}
					// 	}
					// }




					// Make HashMap for Hash BNLJ ////////////////////////////////////
					// table1JoinCol Value -> index in buffer
					HashMap<Integer, List<int[]>> bufferMap = new HashMap<>();
					while (!input.isEmpty()) {
						List<int[]> listOfIndices = new ArrayList<>();

						int[] table1Row = input.remove();
						int value = table1Row[this.table1JoinCol];
						
						if (bufferMap.containsKey(value)) {
							listOfIndices = bufferMap.get(value);
						}
						listOfIndices.add(table1Row);
						
						bufferMap.put(value, listOfIndices);
					}
					
					// just have to loop through table 2
					Iterator<Queue<int[]>> table2RowBlocks = source2.iterator();
					while (table2RowBlocks.hasNext()) {
						Queue<int[]> table2RowBlock = table2RowBlocks.next();
						for (int[] table2Row : table2RowBlock) {
							int value = table2Row[this.table2JoinCol];
							if (bufferMap.containsKey(value)) {
								List<int[]> table1MatchingRows = bufferMap.get(value);
								
								for (int[] table1MatchingRow : table1MatchingRows) {
									rowsToReturn.add(combineRows(table1MatchingRow, table2Row));
								}
							}
							// else, there are no matches

						}
					}
					
					// REGULAR BLOCK NESTED LOOP JOIN
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
