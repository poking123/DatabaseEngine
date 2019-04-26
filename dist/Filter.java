
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class Filter extends RAOperation {
	private Iterable<Queue<int[]>> source;
	private FilterPredicate predicate;

	private String type;
	
	public Filter(RAOperation input, FilterPredicate p) {
		this.source = input;
		this.predicate = p;
		this.type = "filter";
		// this.colsToKeep = colsToKeep;
	}
	
	String getType() {
		return this.type;
	}
	
	@Override
	public Iterator<Queue<int[]>> iterator() {
		return new FilterIterator(source.iterator());
	}
	

	public class FilterIterator implements Iterator<Queue<int[]>> {
		private Iterator<Queue<int[]>> sourceIterator;
		// private int[] colsToKeep;
		
		public FilterIterator(Iterator<Queue<int[]>> sourceIterator) {
			this.sourceIterator = sourceIterator;
			// this.colsToKeep = colsToKeep;
		}
		
		@Override
		public boolean hasNext() {
			return sourceIterator.hasNext();
		}

		@Override
		public Queue<int[]> next() {
			// OLD FILTER
			//////////////////////////////////
			// System.out.println("Filter next called");
			// Queue<int[]> input = sourceIterator.next();
			// Queue<int[]> rowsToReturn = new LinkedList<>();
			// // System.out.println("input is " + input.size());
			// if (input.isEmpty()) {
			// 	// System.out.println("filter input is empty");
			// 	return rowsToReturn;
			// } else {
			// 	// System.out.println("filter - testing ");
			// 	for (int[] row : input) {
			// 		if (predicate.test(row)) {
			// 			// int[] newRow = new int[this.colsToKeep.length];
			// 			// for (int i = 0; i < newRow.length; i++) {
			// 			// 	newRow[i] = row[this.colsToKeep[i]];
			// 			// }
			// 			// rowsToReturn.add(newRow);

			// 			rowsToReturn.add(row);
			// 		}	
			// 	}
			// 	// for (int[] row : rowsToReturn) {
			// 	// 	for (int i : row) {
			// 	// 		System.out.print(i + " ");
			// 	// 	}
					
			// 	// 	System.out.println();
			// 	// }
			// 	// System.out.println();
				
			// 	return rowsToReturn;
			// }
			////////////////////////////////


			Queue<int[]> rowsToReturn = new LinkedList<>();
			while (sourceIterator.hasNext()) {
				Queue<int[]> input = sourceIterator.next();

				while (!input.isEmpty()) {
					int[] row = input.remove();
					if (predicate.test(row)) {
						rowsToReturn.add(row);
					}
				}
				if (rowsToReturn.size() > DatabaseEngine.mergejoinBufferSize) {
					break;
				}

			}
			// System.out.println("Returned Filter");
			return rowsToReturn;
		}

		
	}
	
	

	

	
	
}
