import java.util.Iterator;
import java.util.List;

public class ProjectAndSum implements Iterator<List<int[]>>{
	private Iterator<List<int[]>> source;
	private int[] colsToSum;
	private int[] sums;
	private boolean hasRows;
	
	public ProjectAndSum(Iterator<List<int[]>> input, int[] colsToSum) {
		this.source = input;
		this.colsToSum = colsToSum;
		this.sums = new int[colsToSum.length];
		this.hasRows = false;
		
	}
	
	@Override
	public boolean hasNext() {
		return source.hasNext();
	}

	@Override
	public List<int[]> next() {
		List<int[]> input = source.next();
		
		for (int[] row : input) {
			// System.out.println("Summing");
			hasRows = true;
			for (int i = 0; i < colsToSum.length; i++) {
				int keepIndex = colsToSum[i];
				this.sums[i] += row[keepIndex];
			}
		}
		return null;
	}
	
	public String getSumString() {
		// System.out.println();
		// System.out.println();
		// System.out.println();
		// System.out.println("HERE");
		// for (int i : colsToSum)
		// 	System.out.println(i + " ");

		StringBuilder sb = new StringBuilder();
		if (hasRows) {
			for (int i = 0; i < sums.length - 1; i++) {
				sb.append(sums[i] + ",");
			}
			sb.append(sums[sums.length - 1]);
			return sb.toString();
		} else {
			for (int i = 0; i < sums.length - 1; i++) {
				sb.append(",");
			}
			return sb.toString();
		}
		
	
		
	}

}
