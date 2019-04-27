
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
	private long[] sums;
	private int[] colsToSum;
	
	
	public Scan(String tableName, int[] colsToSum) throws FileNotFoundException {
		this.tableName = tableName;
		this.colsToSum = colsToSum;
		if (this.colsToSum != null) {
			sums = new long[this.colsToSum.length];
		}
	}

	public String toString() {
		return tableName;
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
				
				if (colsToSum != null) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < sums.length - 1; i++) {
						sb.append(",");
					}
					System.out.println(sb.toString());
				}

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
							// System.out.print(row[i] + " ");
						}
						// System.out.println();
						rowsBuffer.add(row);
						rowsRemaining--;
					}
					
				} catch (IOException e) { // Done reading the table
					
				}
				return rowsBuffer;
			}
			
		}
}
