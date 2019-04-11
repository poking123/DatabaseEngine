
public class EquijoinPredicate extends Predicate{

	private int table1JoinCol;
	private int table2JoinCol;
	private boolean isTwoTableJoin;

	private String type;
	
	public EquijoinPredicate(int table1JoinCol, int table2JoinCol, boolean isTwoTableJoin) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;
		this.isTwoTableJoin = isTwoTableJoin;
		this.type = "equijoinPredicate";
	}

	public EquijoinPredicate(int table1JoinCol, int table2JoinCol, boolean isTwoTableJoin, String type) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;
		this.isTwoTableJoin = isTwoTableJoin;
		this.type = type;
	}
	
	@Override
	String getType() {
		return this.type;
	}

	public boolean isTwoTableJoin() {
		return this.isTwoTableJoin;
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
