
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FilterProjectScan extends RAOperation {

	private Queue<ArrayList<int[]>> list = new LinkedList<>();
    private String tableName;
    private int[] colsToKeep;
    private FilterPredicate predicate;
	
	
	public FilterProjectScan(FilterPredicate p) throws FileNotFoundException {
        this.predicate = p;
        this.tableName = predicate.getTableName();
        this.colsToKeep = predicate.getColumnsToKeep();
	}

	public String toString() {
		return tableName;
	}
	
	public String getType() {
		return "projectScan";
	}
	
	@Override
	public Iterator<Queue<int[]>> iterator() {
		try {
			return new FilterProjectScanIterator(list, tableName, colsToKeep);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public class FilterProjectScanIterator implements Iterator<Queue<int[]>> {		
			private final DataInputStream dis;
			private final int numCols;
            private int rowsRemaining;
            private int[] colsToKeep;
			
			public FilterProjectScanIterator(Queue<ArrayList<int[]>> rowsBuffer, String tableName, int[] colsToKeep) throws FileNotFoundException {
				this.dis = Catalog.openStream(tableName);
				this.numCols = Catalog.getColumns(tableName);
                this.rowsRemaining = Catalog.getRows(tableName);
                this.colsToKeep = colsToKeep;
			}
			
			@Override
			public boolean hasNext() {
				if (this.rowsRemaining > 0)
					return true;
				
				return false;
			}
	
			@Override
			public Queue<int[]> next() {
				Queue<int[]> rowsBuffer = new LinkedList<int[]>();
				try {
					while (rowsBuffer.size() < DatabaseEngine.mergejoinBufferSize) {
						int[] oldRow = new int[this.numCols];
						int[] newRow = new int[this.colsToKeep.length];
						for (int i = 0; i < this.numCols; i++) {
							oldRow[i] = dis.readInt();
						}
                        
                        for (int i = 0; i < this.colsToKeep.length; i++) {
                            newRow[i] = oldRow[this.colsToKeep[i]]; // only saves the columns we want
                        }

                        if (predicate.test(newRow)) {
                            rowsBuffer.add(newRow);
                        }
						rowsRemaining--; // need to always decrease the amount of rows
					}
					
				} catch (IOException e) { // Done reading the table
					
				}
				return rowsBuffer;
			}
			
		}
}
