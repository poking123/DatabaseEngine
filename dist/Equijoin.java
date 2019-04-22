import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;

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

		private Queue<int[]> table1MatchingRows;
		private int[] table2Row;

		private Iterator<Queue<int[]>> table2RowBlocks;

		private Queue<int[]> table2RowBlock;

		private HashMap<Integer, Queue<int[]>> bufferMap;
		
		public EquijoinIterator(Iterator<Queue<int[]>> source1Iterator, Iterable<Queue<int[]>> source2, boolean isTwoTableJoin, int table1JoinCol, int table2JoinCol) {
			this.source1Iterator = source1Iterator;
			this.source2 = source2;
			this.isTwoTableJoin = isTwoTableJoin;
			
			this.table1JoinCol = table1JoinCol;
			this.table2JoinCol = table2JoinCol;

			this.table1MatchingRows = new LinkedList<>();
			table2Row = new int[1];

			if (this.isTwoTableJoin) {
				table2RowBlocks = source2.iterator();
				if (table2RowBlocks.hasNext()) {
					table2RowBlock = table2RowBlocks.next();
				}
			}
			

			

			bufferMap = new HashMap<>();
		}
		
		@Override
		public boolean hasNext() {
			return source1Iterator.hasNext() || this.table1MatchingRows.size() > 0;
		}

		@Override
		public Queue<int[]> next() {
				
			Queue<int[]> rowsToReturn = new LinkedList<>();

			if (this.isTwoTableJoin) {
				// finish the rest of the matching rows
				if (this.table1MatchingRows.size() > 0) {
					while (!this.table1MatchingRows.isEmpty()) {
						rowsToReturn.add(combineRows(this.table1MatchingRows.remove(), this.table2Row));

						if (rowsToReturn.size() > DatabaseEngine.bufferSize) {
							return rowsToReturn;
						}
					}
				}

				// finish the rest of the table2 Block
				while (!table2RowBlock.isEmpty()) {
					this.table2Row = table2RowBlock.remove();
					int value = table2Row[this.table2JoinCol];
					if (bufferMap.containsKey(value)) {
						this.table1MatchingRows = new LinkedList<>(bufferMap.get(value));
						
						while (!this.table1MatchingRows.isEmpty()) {
							// int[] table1MatchingRow = table1MatchingRows.remove();
							rowsToReturn.add(combineRows(table1MatchingRows.remove(), table2Row));

							if (rowsToReturn.size() > DatabaseEngine.bufferSize) {
								return rowsToReturn;
							}
						}
					}
					// else, there are no matches
				}

				// finished the rest of table2 Blocks
				while (table2RowBlocks.hasNext() && !bufferMap.isEmpty()) {
					table2RowBlock = table2RowBlocks.next();
					while (!table2RowBlock.isEmpty()) {
						this.table2Row = table2RowBlock.remove();
						int value = table2Row[this.table2JoinCol];
						if (bufferMap.containsKey(value)) {
							this.table1MatchingRows = new LinkedList<>(bufferMap.get(value));
							
							while (!this.table1MatchingRows.isEmpty()) {
								// int[] table1MatchingRow = table1MatchingRows.remove();
								rowsToReturn.add(combineRows(table1MatchingRows.remove(), table2Row));

								if (rowsToReturn.size() > DatabaseEngine.bufferSize) {
									return rowsToReturn;
								}
							}
						}
						// else, there are no matches

					}
				}
			}

			

			// next table1 Block
			Queue<int[]> input = new LinkedList<>();
			if (source1Iterator.hasNext()) { // we will have a new block
				input = source1Iterator.next();
			}
			
			
			if (input.isEmpty()) { // this will also catch when we hasNext() is not true
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
					bufferMap = new HashMap<>();
					while (!input.isEmpty()) {
						Queue<int[]> listOfIndices = new LinkedList<>();

						int[] table1Row = input.remove();
						int value = table1Row[this.table1JoinCol];
						
						if (bufferMap.containsKey(value)) {
							listOfIndices = bufferMap.get(value);
						}
						listOfIndices.add(table1Row);
						
						bufferMap.put(value, listOfIndices);
					}

					
					// just have to loop through table 2
					table2RowBlocks = source2.iterator();
					while (table2RowBlocks.hasNext()) {
						table2RowBlock = table2RowBlocks.next();
						while (!table2RowBlock.isEmpty()) {
							this.table2Row = table2RowBlock.remove();
							int value = table2Row[this.table2JoinCol];
							if (bufferMap.containsKey(value)) {
								this.table1MatchingRows = new LinkedList<>(bufferMap.get(value));
								
								while (!this.table1MatchingRows.isEmpty()) {
									// int[] table1MatchingRow = table1MatchingRows.remove();
									rowsToReturn.add(combineRows(table1MatchingRows.remove(), table2Row));

									if (rowsToReturn.size() > DatabaseEngine.bufferSize) {
										return rowsToReturn;
									}
								}
							}
							// else, there are no matches

						}
					}



					
					// REGULAR BLOCK NESTED LOOP JOIN
					// for (int[] table1Row : input) {
					// 	Iterator<Queue<int[]>> table2RowBlocks = source2.iterator();
					// 	while (table2RowBlocks.hasNext()) {
					// 		Queue<int[]> table2RowBlock = table2RowBlocks.next();
					// 		for (int[] table2Row : table2RowBlock) {
					// 			if (equijoinPredicate.test(table1Row, table2Row)) {
					// 				rowsToReturn.add(combineRows(table1Row, table2Row));
					// 			}	
					// 		}
					// 	}
					// }
					
					
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

			System.arraycopy(row1, 0, combinedRow, 0, row1.length);
			System.arraycopy(row2, 0, combinedRow, row1.length, row2.length);
			
			return combinedRow;
		}
		
	}

	
	
	
	
	
	
	
}
