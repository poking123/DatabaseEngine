
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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

			public void print(int[] arr) {
				for (int i : arr)
					System.err.print(i + " ");
				System.err.println();
			}
			
			@Override
			public boolean hasNext() {
				if (this.rowsRemaining > 0)
					return true;
				
				return false;
			}

			public int fromByteArray(byte[] bytes) {
				return ByteBuffer.wrap(bytes).getInt();
		   }

			public int byteArrayToInt(byte[] b) {
				return b[0] << 24 | (b[1] & 0xff) << 16 | (b[2] & 0xff) << 8 | (b[3] & 0xff);
	 		}
	
			@Override
			public Queue<int[]> next() {
				Queue<int[]> rowsBuffer = new LinkedList<int[]>();
				System.err.println("Starting filter");
				// System.err.println("rowsRemaining is " + this.rowsRemaining);
				// System.err.println("hasNext is " + (this.rowsRemaining > 0));
				long start = System.currentTimeMillis();
				int rowBufferSize = DatabaseEngine.scanBufferSize;
				try {
					
					while (rowsBuffer.size() < DatabaseEngine.bufferSize && rowsRemaining > 0) {

						byte[] buffer = new byte[4 * numCols * rowBufferSize];

						int bytesRead = dis.read(buffer, 0, 4 * numCols * rowBufferSize);
						// System.out.println("row buffer size is " + rowBufferSize);
						// System.out.println("rowsRemaining is " + rowsRemaining);
						for (int j = 0; j < bytesRead / 4; j += this.numCols) {
							int[] oldRow = new int[numCols];
							for (int i = 0; i < numCols; i++) {
								byte[] newByteArr = Arrays.copyOfRange(buffer, 4 * i + j * 4, 4 * i + 4 + j * 4);
								int value = byteArrayToInt(newByteArr);
								oldRow[i] = value;
							}

							int[] newRow = new int[this.colsToKeep.length];
							
							for (int i = 0; i < this.colsToKeep.length; i++) {
								newRow[i] = oldRow[this.colsToKeep[i]]; // only saves the columns we want
							}

							if (predicate.test(newRow)) {
								rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
							}
							rowsRemaining--;
						}
					}
					
				} catch (IOException e) { // Done reading the table
					
				}
				long stop = System.currentTimeMillis();
				System.err.println("Filter time: " + (stop - start));
				// System.err.println("filter returned222");
				// System.err.println("rowsBuffer size is " + rowsBuffer.size());
				// System.err.println("rowsRemaining is " + this.rowsRemaining);
				return rowsBuffer;
			}
			
		}
}
