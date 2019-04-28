
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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

			public int fromByteArray(byte[] bytes) {
				return ByteBuffer.wrap(bytes).getInt();
		   }

		   public int byteArrayToInt(byte[] b) {
			  	return b[0] << 24 | (b[1] & 0xff) << 16 | (b[2] & 0xff) << 8 | (b[3] & 0xff);
		   }
	
			@Override
			public Queue<int[]> next() {
				Queue<int[]> rowsBuffer = new LinkedList<int[]>();
				int rowBufferSize = DatabaseEngine.scanBufferSize;
				long start = System.currentTimeMillis();

				try {
					while (rowsBuffer.size() < DatabaseEngine.bufferSize && rowsRemaining > 0) {

						byte[] buffer = new byte[4 * numCols * rowBufferSize];

						int bytesRead = dis.read(buffer, 0, 4 * numCols * rowBufferSize);
						for (int j = 0; j < bytesRead / 4; j += this.numCols) {
							int[] row = new int[numCols];
							for (int i = 0; i < numCols; i++) {
								byte[] newByteArr = Arrays.copyOfRange(buffer, 4 * i + j * 4, 4 * i + 4 + j * 4);
								int value = byteArrayToInt(newByteArr);
								row[i] = value;
							}

							rowsBuffer.add(Arrays.copyOf(row, row.length));
							rowsRemaining--;
						}
						
					}
					
				} catch (IOException e) { // Done reading the table
					
				}
				long stop = System.currentTimeMillis();
				System.err.println("Scan time " + (stop - start));
				return rowsBuffer;
			}
			
		}
}
