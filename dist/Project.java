import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Project extends RAOperation implements Iterable<Queue<int[]>>{
	private Iterable<Queue<int[]>> source;
	private int[] colsToKeep;
	
	public Project(Iterable<Queue<int[]>> input, int[] colsToKeep) {
		this.source = input;
		this.colsToKeep = colsToKeep;
	}
	
	public String getType() {
		return "project";
	}
	
	@Override
	public Iterator<Queue<int[]>> iterator() {
		return new ProjectIterator(source.iterator());
	}
	
	
	public class ProjectIterator implements Iterator<Queue<int[]>> {
		private Iterator<Queue<int[]>> sourceIterator;
		
		public ProjectIterator(Iterator<Queue<int[]>> sourceIterator) {
			this.sourceIterator = sourceIterator;
		}
		
		@Override
		public boolean hasNext() {
			return sourceIterator.hasNext();
		}
		@Override
		public Queue<int[]> next() {
			Queue<int[]> input = sourceIterator.next();
			Queue<int[]> rowsToReturn = new LinkedList<>();
			
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
