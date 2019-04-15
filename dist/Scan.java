
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class Scan extends RAOperation {

	private Queue<ArrayList<int[]>> list = new LinkedList<>();
	private String tableName;
	
	
	public Scan(String tableName) throws FileNotFoundException {
		this.tableName = tableName;
	}

	public String toString() {
		return "Scan " + tableName;
	}
	
	public String getType() {
		return "scan";
	}
	
	@Override
	public Iterator<Queue<int[]>> iterator() {
		try {
			return new ScanIterator(list, tableName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public class ScanIterator implements Iterator<Queue<int[]>> {
			
//			private final int bufferSize = 50;
			
			private final DataInputStream dis;
			private final int numCols;
			private int rowsRemaining;
			
			public ScanIterator(Queue<ArrayList<int[]>> rowsBuffer, String tableName) throws FileNotFoundException {
				this.dis = Catalog.openStream(tableName);
				this.numCols = Catalog.getColumns(tableName);
				this.rowsRemaining = Catalog.getRows(tableName);
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
					while (rowsBuffer.size() < DatabaseEngine.bufferSize) {
						int[] row = new int[numCols];
						for (int i = 0; i < numCols; i++) {
							row[i] = dis.readInt();
						}
						rowsBuffer.add(row);
						rowsRemaining--;
					}
					
				} catch (IOException e) { // Done reading the table
					
				}
				return rowsBuffer;
			}
			
		}
}
