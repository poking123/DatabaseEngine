
public class MergeJoinPredicate extends Predicate {

	private int table1JoinCol;
	private int table2JoinCol;

	private String type;
	
	public MergeJoinPredicate(int table1JoinCol, int table2JoinCol) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;
		this.type = "mergeJoinPredicate";
	}

	public String toString() {
		return this.type + " - JoinCols: " + table1JoinCol + " " + table2JoinCol;
	}
	
	public int getTable1JoinCol() {
		return this.table1JoinCol;
	}
	
	public int getTable2JoinCol() {
		return this.table2JoinCol;
	}
	
	@Override
	String getType() {
		return this.type;
	}
	
	public boolean test(int[] table1Row, int[] table2Row) {
		// System.out.println("table1JoinCol is " + table1JoinCol);
		// System.out.println("table2JoinCol is " + table2JoinCol);
		return table1Row[table1JoinCol] == table2Row[table2JoinCol];
	}

	public boolean test(int[] table1Row) { // join columns are on the same table
		return table1Row[table1JoinCol] == table1Row[table2JoinCol];
	}

}
