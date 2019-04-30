
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
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
		return new ScanIterator(list, tableName);

		// try {
		// 	return new ScanIterator(list, tableName);
		// } catch (FileNotFoundException e) {
		// 	e.printStackTrace();
		// 	return null;
		// }
	}
	
	public class ScanIterator implements Iterator<Queue<int[]>> {		
			// private final DataInputStream dis;
			private MappedByteBuffer mbb;
			private final int numCols;
			private int rowsRemaining;
			private ByteBuffer bb;
			
			public ScanIterator(Queue<ArrayList<int[]>> rowsBuffer, String tableName) {
				// this.dis = Catalog.openStream(tableName);
				try {
					this.mbb = Catalog.openChannel(tableName);
				} catch (IOException e) {
					
				}
				this.numCols = Catalog.getColumns(tableName);
				this.rowsRemaining = Catalog.getRows(tableName);
				this.bb = ByteBuffer.allocate(DatabaseEngine.byteBufferSize);
				this.bb.flip();
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
				// int rowBufferSize = DatabaseEngine.scanBufferSize;
				// long start = System.currentTimeMillis();

				
				while (rowsBuffer.size() < DatabaseEngine.bufferSize && rowsRemaining > 0) {

					// int index = 0;

					// int[] oldRow = new int[this.numCols];
					// while (index < this.numCols && bb.hasRemaining() && rowsRemaining > 0) {
					// 	int value = bb.getInt();
					// 	oldRow[index] = value;
					// 	index = (index + 1) % this.numCols;
					// 	if (index == 0) {
					// 		rowsBuffer.add(Arrays.copyOf(oldRow, oldRow.length));
					// 		rowsRemaining--;
					// 	}
					// }


					// boolean finishRow = (index != 0);

					// byte[] buffer = new byte[DatabaseEngine.dataInputBufferSize];
					// dis.read(buffer);
					// bb = MappedByteBuffer.wrap(buffer);

					// if (finishRow) {
					// 	while (index < this.numCols) {
					// 		oldRow[index] = bb.getInt();
					// 		index++;
					// 	}
					// 	rowsBuffer.add(Arrays.copyOf(oldRow, oldRow.length));
					// 	rowsRemaining--;
					// }
					//////////////////////////////////////

					
					int index = 0;
					int[] newRow = new int[this.numCols];
					while (index < this.numCols) {
						int value = mbb.getInt();
						newRow[index] = value;
						index++;
					}
					rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
					rowsRemaining--;
					index = 0;
					
					
				}
					
				
				// long stop = System.currentTimeMillis();
				// System.err.println("Scan time " + (stop - start));
				// if (this.rowsRemaining == 0) {
				// 	try {
				// 		dis.close();
				// 	} catch (IOException e) {
					
				// 	}
				// }
				
				return rowsBuffer;
			}
			
		}
}
