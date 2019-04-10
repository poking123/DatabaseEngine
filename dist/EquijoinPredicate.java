
public class EquijoinPredicate extends Predicate{

	private int table1JoinCol;
	private int table2JoinCol;
	private boolean isTwoTableJoin;
	
	public EquijoinPredicate(int table1JoinCol, int table2JoinCol, boolean isTwoTableJoin) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;
		this.isTwoTableJoin = isTwoTableJoin;
	}
	
	@Override
	String getType() {
		return "equijoinPredicate";
	}

	public boolean isTwoTableJoin() {
		return this.isTwoTableJoin;
	}
	
	public boolean test(int[] table1Row, int[] table2Row) {
		return table1Row[table1JoinCol] == table2Row[table2JoinCol];
	}

	public boolean test(int[] table1Row) { // join columns are on the same table
		return table1Row[table1JoinCol] == table1Row[table2JoinCol];
	}

}
