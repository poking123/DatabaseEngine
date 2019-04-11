
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Filter extends RAOperation {
	private Iterable<List<int[]>> source;
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
	public Iterator<List<int[]>> iterator() {
		return new FilterIterator(source.iterator());
	}
	

	public class FilterIterator implements Iterator<List<int[]>> {
		private Iterator<List<int[]>> sourceIterator;
		
		public FilterIterator(Iterator<List<int[]>> sourceIterator) {
			this.sourceIterator = sourceIterator;
		}
		
		@Override
		public boolean hasNext() {
			return sourceIterator.hasNext();
		}

		@Override
		public List<int[]> next() {
			List<int[]> input = sourceIterator.next();
			List<int[]> rowsToReturn = new ArrayList<>();
			
			if (input.isEmpty()) {
				return rowsToReturn;
			} else {
				for (int[] row : input) {
					if (predicate.test(row)) {
						rowsToReturn.add(row);
					}	
				}
				return rowsToReturn;
			}
		}
	}
	
	

	

	
	
}
