import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Project extends RAOperation implements Iterable<List<int[]>>{
	private Iterable<List<int[]>> source;
	private int[] colsToKeep;
	
	public Project(Iterable<List<int[]>> input, int[] colsToKeep) {
		this.source = input;
		this.colsToKeep = colsToKeep;
	}
	
	public String getType() {
		return "project";
	}
	
	@Override
	public Iterator<List<int[]>> iterator() {
		return new ProjectIterator(source.iterator());
	}
	
	
	public class ProjectIterator implements Iterator<List<int[]>> {
		private Iterator<List<int[]>> sourceIterator;
		
		public ProjectIterator(Iterator<List<int[]>> sourceIterator) {
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
					int[] newRow = new int[colsToKeep.length];
					for (int i = 0; i < colsToKeep.length; i++) {
						int keepIndex = colsToKeep[i];
						newRow[i] = row[keepIndex];
					}
					rowsToReturn.add(newRow);
				}
				
				return rowsToReturn;
			}
			
			
		}
	}
	
}
