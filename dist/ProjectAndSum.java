import java.util.Iterator;
import java.util.Queue;

public class ProjectAndSum implements Iterator<Queue<int[]>>{
	private Iterator<Queue<int[]>> source;
	private int[] colsToSum;
	private long[] sums;
	private boolean hasRows;
	
	public ProjectAndSum(Iterator<Queue<int[]>> input, int[] colsToSum) {
		this.source = input;
		this.colsToSum = colsToSum;
		this.sums = new long[colsToSum.length];
		// this.hasRows = false;
	}
	
	@Override
	public boolean hasNext() {
		return source.hasNext();
	}

	@Override
	public Queue<int[]> next() {
		// System.out.println("PAS next called");
		Queue<int[]> input = source.next();
		
		for (int[] row : input) {
			// System.out.println("Summing");
			hasRows = true;
			for (int i = 0; i < colsToSum.length; i++) {
				int keepIndex = colsToSum[i];
				this.sums[i] += row[keepIndex];
			}
			// DatabaseEngine.finalNumber++;
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

		// System.out.println(DatabaseEngine.finalNumber);

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
			// return "no results";
			return sb.toString();
		}
		
	
		
	}

}
