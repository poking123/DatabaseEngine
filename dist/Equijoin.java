import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Equijoin implements Iterable<List<int[]>> {
	private Iterable<List<int[]>> source1;
	private Iterable<List<int[]>> source2;
	
	private EquijoinPredicate equijoinPredicate;

	public Equijoin(Iterable<List<int[]>> source1, Iterable<List<int[]>> source2, EquijoinPredicate equijoinPredicate) {
		this.source1 = source1;
		this.source2 = source2;
		this.equijoinPredicate = equijoinPredicate;
	}
	
	@Override
	public Iterator<List<int[]>> iterator() {
		return new EquijoinIterator(source1.iterator(), source2);
	}

	public class EquijoinIterator implements Iterator<List<int[]>> {
		private Iterator<List<int[]>> source1Iterator;
		private Iterable<List<int[]>> source2;
		
		public EquijoinIterator(Iterator<List<int[]>> source1Iterator, Iterable<List<int[]>> source2) {
			this.source1Iterator = source1Iterator;
			this.source2 = source2;
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
				
				
				for (int[] table1Row : input) {
					// for (int i : table1Row) {
					// 	System.out.print(i + " ");
					// }	
					// System.out.println();
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
				return rowsToReturn;
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
