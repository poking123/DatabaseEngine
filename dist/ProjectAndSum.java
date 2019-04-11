import java.util.Iterator;
import java.util.List;

public class ProjectAndSum implements Iterator<List<int[]>>{
	private Iterator<List<int[]>> source;
	private int[] colsToSum;
	private int[] sums;
	
	public ProjectAndSum(Iterator<List<int[]>> input, int[] colsToSum) {
		this.source = input;
		this.colsToSum = colsToSum;
		this.sums = new int[colsToSum.length];
		System.out.println("Look Here");
		for (int i : colsToSum)
			System.out.print(i + " ");
		System.out.println();
	}
	
	@Override
	public boolean hasNext() {
		System.out.println("proehct and sum hasNext() called");
		return source.hasNext();
	}

	@Override
	public List<int[]> next() {
		List<int[]> input = source.next();
		
		for (int[] row : input) {
			for (int i = 0; i < colsToSum.length; i++) {
				int keepIndex = colsToSum[i];
				this.sums[i] += row[keepIndex];
				System.out.println("Currently adding sums");
			}
		}
		return null;
	}
	
	public String getSumString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < sums.length - 1; i++) {
			sb.append(sums[i] + ",");
		}
		sb.append(sums[sums.length - 1]);
		return sb.toString();
	}

}
