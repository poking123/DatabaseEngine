import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FilterProjectScan extends RAOperation {

	private Queue<ArrayList<int[]>> list = new LinkedList<>();
    private String tableName;
    private int[] colsToKeep;
	private FilterPredicate predicate;
	private HashSet<Integer> colsToKeepSet;
	
	
	public FilterProjectScan(FilterPredicate p) throws FileNotFoundException {
        this.predicate = p;
        this.tableName = predicate.getTableName();
		this.colsToKeep = predicate.getColumnsToKeep();
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
			return new FilterProjectScanIterator(list, tableName, colsToKeep);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public class FilterProjectScanIterator implements Iterator<Queue<int[]>> {		
			// private final DataInputStream dis;
			private MappedByteBuffer mbb;
			private final int numCols;
            private int rowsRemaining;
			// private ByteBuffer bb;
			
			public FilterProjectScanIterator(Queue<ArrayList<int[]>> rowsBuffer, String tableName, int[] colsToKeep) throws FileNotFoundException {
				// this.dis = Catalog.openStream(tableName);
				try {
					this.mbb = Catalog.openChannel(tableName);
				} catch (IOException e) {
					
				}
				this.numCols = Catalog.getColumns(tableName);
                this.rowsRemaining = Catalog.getRows(tableName);
				// this.bb = ByteBuffer.allocate(DatabaseEngine.byteBufferSize);
				// this.bb.flip();
			}

			public void print(int[] arr) {
				for (int i : arr)
					System.err.print(i + " ");
				System.err.println();
			}
			
			@Override
			public boolean hasNext() {
				return (this.rowsRemaining > 0);
			}
	
			@Override
			public Queue<int[]> next() {
				Queue<int[]> rowsBuffer = new LinkedList<int[]>();
				// System.err.println("Starting filter");
				// System.err.println("rowsRemaining is " + this.rowsRemaining);
				// System.err.println("hasNext is " + (this.rowsRemaining > 0));
				// long start = System.currentTimeMillis();
				// int rowBufferSize = DatabaseEngine.scanBufferSize;
				
					
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
					// 		if (predicate.test(newRow)) {
					// 			rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
					// 		}
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

					// 	if (predicate.test(newRow)) {
					// 		rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
					// 	}
					// 	rowsRemaining--;
					// }


					int index = 0;
					int colsToKeepIndex = 0;

					int[] newRow = new int[colsToKeep.length];
					while (index < this.numCols) {
						int value = mbb.getInt();
						if (colsToKeepSet.contains(index)) {
							newRow[colsToKeepIndex] = value;
							colsToKeepIndex++;
						}
						index++;
					}
					if (predicate.test(newRow)) {
						// rowsBuffer.add(Arrays.copyOf(newRow, newRow.length));
						rowsBuffer.add(newRow);
					}
					rowsRemaining--;
					index = 0;
					colsToKeepIndex = 0;
				}
					
				
				// long stop = System.currentTimeMillis();
				// System.err.println("Filter time: " + (stop - start));
				// System.err.println("filter returned222");
				// System.err.println("rowsBuffer size is " + rowsBuffer.size());
				// System.err.println("rowsRemaining is " + this.rowsRemaining);
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
