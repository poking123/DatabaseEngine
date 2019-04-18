import java.util.Iterator;
import java.util.Queue;
import java.util.LinkedList;

public class Project extends RAOperation {
	private Iterable<Queue<int[]>> source;
	private int[] colsToKeep;
	
	public Project(Iterable<Queue<int[]>> input, int[] colsToKeep) {
		this.source = input;
		this.colsToKeep = colsToKeep;
	}
	
	

    @Override
    public Iterator<Queue<int[]>> iterator() {
        return new ProjectIterator(source.iterator(), colsToKeep);
    }

    @Override
    String getType() {
        return "project";
    }


    public class ProjectIterator implements Iterator<Queue<int[]>> {
        private Iterator<Queue<int[]>> sourceIterator;
        private int[] colsToKeep;
		
		public ProjectIterator(Iterator<Queue<int[]>> sourceIterator, int[] colsToKeep) {
            this.sourceIterator = sourceIterator;
            this.colsToKeep = colsToKeep;
		}
		
		@Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public Queue<int[]> next() {
            Queue<int[]> input = sourceIterator.next();
            Queue<int[]> rowsToKeep = new LinkedList<>();

            while (!input.isEmpty()) {
                int[] oldRow = input.remove();
                int[] newRow = new int[this.colsToKeep.length];

                for (int i = 0; i < this.colsToKeep.length; i++) {
                    newRow[i] = oldRow[this.colsToKeep[i]]; // only saves the columns we want
                    // System.out.print(newRow[i] + " ");
                }
                // System.out.println();
                rowsToKeep.add(newRow);
            }

            return rowsToKeep;
        }
	}

}
