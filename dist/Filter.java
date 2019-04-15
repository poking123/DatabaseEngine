
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
		
		public FilterIterator(Iterator<Queue<int[]>> sourceIterator) {
			this.sourceIterator = sourceIterator;
		}
		
		@Override
		public boolean hasNext() {
			return sourceIterator.hasNext();
		}

		@Override
		public Queue<int[]> next() {
			// System.out.println("Filter next called");
			Queue<int[]> input = sourceIterator.next();
			Queue<int[]> rowsToReturn = new LinkedList<>();
			
			if (input.isEmpty()) {
				// System.out.println("filter input is empty");
				return rowsToReturn;
			} else {
				// System.out.println("filter - testing ");
				for (int[] row : input) {
					if (predicate.test(row)) {
						rowsToReturn.add(row);
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
			}
		}
	}
	
	

	

	
	
}
