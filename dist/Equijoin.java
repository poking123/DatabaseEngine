import java.util.ArrayList;
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
		return new EquijoinIterator(source1.iterator(), source2, equijoinPredicate.isTwoTableJoin());
	}

	public class EquijoinIterator implements Iterator<List<int[]>> {
		private Iterator<List<int[]>> source1Iterator;
		private Iterable<List<int[]>> source2;
		private boolean isTwoTableJoin;
		
		public EquijoinIterator(Iterator<List<int[]>> source1Iterator, Iterable<List<int[]>> source2, boolean isTwoTableJoin) {
			this.source1Iterator = source1Iterator;
			this.source2 = source2;
			this.isTwoTableJoin = isTwoTableJoin;
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
					for (int[] table1Row : input) {
						Iterator<List<int[]>> table2RowBlocks = source2.iterator();
						while (table2RowBlocks.hasNext()) {
							List<int[]> table2RowBlock = table2RowBlocks.next();
							for (int[] table2Row : table2RowBlock) {
								if (equijoinPredicate.test(table1Row, table2Row)) {
									rowsToReturn.add(combineRows(table1Row, table2Row));
								}	
							}
						}
					}
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
