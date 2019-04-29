
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class ProjectScan extends RAOperation {

	private Queue<ArrayList<int[]>> list = new LinkedList<>();
    private String tableName;
    private int[] colsToKeep;
	
	
	public ProjectScan(String tableName, int[] colsToKeep) throws FileNotFoundException {
        this.tableName = tableName;
        this.colsToKeep = colsToKeep;
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
			return new ProjectScanIterator(list, tableName, colsToKeep);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public class ProjectScanIterator implements Iterator<Queue<int[]>> {		
			private final DataInputStream dis;
			private final int numCols;
            private int rowsRemaining;
			private int[] colsToKeep;
			private ByteBuffer bb;
			
			public ProjectScanIterator(Queue<ArrayList<int[]>> rowsBuffer, String tableName, int[] colsToKeep) throws FileNotFoundException {
				this.dis = Catalog.openStream(tableName);
				this.numCols = Catalog.getColumns(tableName);
                this.rowsRemaining = Catalog.getRows(tableName);
				this.colsToKeep = colsToKeep;
				this.bb = ByteBuffer.allocate(DatabaseEngine.byteBufferSize);
				this.bb.flip();
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
				// int rowBufferSize = DatabaseEngine.scanBufferSize;
				long start = System.currentTimeMillis();
				try {
					while (rowsBuffer.size() < DatabaseEngine.bufferSize && rowsRemaining > 0) {

						int index = 0;

						int[] oldRow = new int[this.numCols];
						while (index < this.numCols && bb.hasRemaining() && rowsRemaining > 0) {
							int value = bb.getInt();
							oldRow[index] = value;
							index = (index + 1) % this.numCols;
							if (index == 0) {
								int[] newRow = new int[this.colsToKeep.length];
							
								for (int i = 0; i < this.colsToKeep.length; i++) {
									newRow[i] = oldRow[this.colsToKeep[i]]; // only saves the columns we want
								}
								rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
								rowsRemaining--;
							}
						}


						boolean finishRow = (index != 0);

						byte[] buffer = new byte[DatabaseEngine.dataInputBufferSize];
						dis.read(buffer);
						bb = ByteBuffer.wrap(buffer);

						if (finishRow) {
							while (index < this.numCols) {
								oldRow[index] = bb.getInt();
								index++;
							}
							int[] newRow = new int[this.colsToKeep.length];
							
							for (int i = 0; i < this.colsToKeep.length; i++) {
								newRow[i] = oldRow[this.colsToKeep[i]]; // only saves the columns we want
							}
							rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
							rowsRemaining--;
						}



						// byte[] buffer = new byte[4 * numCols * rowBufferSize];

						// int bytesRead = dis.read(buffer, 0, 4 * numCols * rowBufferSize);

						// for (int j = 0; j < bytesRead / 4; j += this.numCols) {
						// 	int[] oldRow = new int[numCols];
						// 	for (int i = 0; i < numCols; i++) {
						// 		byte[] newByteArr = Arrays.copyOfRange(buffer, 4 * i + j * 4, 4 * i + 4 + j * 4);
						// 		int value = byteArrayToInt(newByteArr);
						// 		oldRow[i] = value;
						// 	}

							// int[] newRow = new int[this.colsToKeep.length];
							
							// for (int i = 0; i < this.colsToKeep.length; i++) {
							// 	newRow[i] = oldRow[this.colsToKeep[i]]; // only saves the columns we want
							// }
							// rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
						// 	rowsRemaining--;
						// }

						
					}
					
				} catch (IOException e) { // Done reading the table
					
				}
				long stop = System.currentTimeMillis();
				System.err.println("ProjectScan time " + (stop - start));
				return rowsBuffer;
			}
			
		}
}
