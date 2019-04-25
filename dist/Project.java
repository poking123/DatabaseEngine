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
	
	public String toString() {
        return this.source.toString();
    }

    @Override
    public Iterator<Queue<int[]>> iterator() {
        return new ProjectIterator(source.iterator(), colsToKeep, this.toString());
    }

    @Override
    String getType() {
        return "project";
    }


    public class ProjectIterator implements Iterator<Queue<int[]>> {
        private Iterator<Queue<int[]>> sourceIterator;
        private int[] colsToKeep;
        private String tableFileName;
		
		public ProjectIterator(Iterator<Queue<int[]>> sourceIterator, int[] colsToKeep, String tableFileName) {
            this.sourceIterator = sourceIterator;
            this.colsToKeep = colsToKeep;
            this.tableFileName = tableFileName;
        }
        
        public String toString() {
            return tableFileName;
        }

        public String getType() {
            return "Project";
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
