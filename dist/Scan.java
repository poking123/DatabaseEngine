
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Scan extends RAOperation implements Iterable<List<int[]>> {

	private List<ArrayList<int[]>> list = new ArrayList<>();
	private String tableName;
	
	
	public Scan(String tableName) throws FileNotFoundException {
		this.tableName = tableName;
	}
	
	public String getType() {
		return "scan";
	}
	
	@Override
	public Iterator<List<int[]>> iterator() {
		try {
			return new ScanIterator(list, tableName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public class ScanIterator implements Iterator<List<int[]>> {
			
			private final int bufferSize = 1000;
			
			private final DataInputStream dis;
			private final int numCols;
			private int rowsRemaining;
			
			public ScanIterator(List<ArrayList<int[]>> rowsBuffer, String tableName) throws FileNotFoundException {
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
			public List<int[]> next() {
				List<int[]> rowsBuffer = new ArrayList<int[]>();
				try {
					while (rowsBuffer.size() < bufferSize) {
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
