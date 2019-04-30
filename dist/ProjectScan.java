import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class ProjectScan extends RAOperation {

	private Queue<ArrayList<int[]>> list = new LinkedList<>();
    private String tableName;
	private int[] colsToKeep;
	private HashSet<Integer> colsToKeepSet;
	
	
	public ProjectScan(String tableName, int[] colsToKeep) throws FileNotFoundException {
        this.tableName = tableName;
		this.colsToKeep = colsToKeep;
		this.colsToKeepSet = new HashSet<>();
		for (int i : colsToKeep) this.colsToKeepSet.add(i);
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
			// private final DataInputStream dis;
			private MappedByteBuffer mbb;
			private final int numCols;
            private int rowsRemaining;
			private int[] colsToKeep;
			// private ByteBuffer bb;
			
			public ProjectScanIterator(Queue<ArrayList<int[]>> rowsBuffer, String tableName, int[] colsToKeep) throws FileNotFoundException {
				// this.dis = Catalog.openStream(tableName);
				try {
					this.mbb = Catalog.openChannel(tableName);
				} catch (IOException e) {
					
				}


				this.numCols = Catalog.getColumns(tableName);
                this.rowsRemaining = Catalog.getRows(tableName);
				this.colsToKeep = colsToKeep;
				// this.bb = ByteBuffer.allocate(DatabaseEngine.byteBufferSize);
				// this.bb.flip();
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
				// int rowBufferSize = DatabaseEngine.scanBufferSize;
				// long start = System.currentTimeMillis();
				
				while (rowsBuffer.size() < DatabaseEngine.bufferSize && rowsRemaining > 0) {

					// int index = 0;
					// int colsToKeepIndex = 0;

					// int[] newRow = new int[colsToKeepSet.size()];
					// while (index < this.numCols && bb.hasRemaining() && rowsRemaining > 0) {
					// 	int value = bb.getInt();
					// 	if (colsToKeepSet.contains(index)) {
					// 		newRow[colsToKeepIndex] = value;
					// 		colsToKeepIndex++;
					// 	}

					// 	index = (index + 1) % this.numCols;
					// 	if (index == 0) {
					// 		colsToKeepIndex = 0;
					// 		rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
					// 		rowsRemaining--;
					// 	}
					// }


					// boolean finishRow = (index != 0);

					// byte[] buffer = new byte[DatabaseEngine.dataInputBufferSize];
					// dis.read(buffer);
					// bb = MappedByteBuffer.wrap(buffer);

					// if (finishRow) {
					// 	while (index < this.numCols) {
					// 		int value = bb.getInt();
					// 		if (colsToKeepSet.contains(index)) {
					// 			newRow[colsToKeepIndex] = value;
					// 			colsToKeepIndex++;
					// 		}
					// 		index++;
					// 	}
					// 	rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
					// 	rowsRemaining--;
					// }

					int index = 0;
					int colsToKeepIndex = 0;

					int[] newRow = new int[this.colsToKeep.length];
					while (index < this.numCols) {
						int value = mbb.getInt();
						if (colsToKeepSet.contains(index)) {
							newRow[colsToKeepIndex] = value;
							colsToKeepIndex++;
						}
						index++;
					}
					rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
					rowsRemaining--;
					index = 0;
					colsToKeepIndex = 0;
					
				}
					
				
				// long stop = System.currentTimeMillis();
				// System.err.println("ProjectScan time " + (stop - start));
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
