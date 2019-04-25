
public class EquijoinWritePredicate extends Predicate {

	private int table1JoinCol;
	private int table2JoinCol;
	private boolean isTwoTableJoin;

	private String type;
	
	public EquijoinWritePredicate(int table1JoinCol, int table2JoinCol, boolean isTwoTableJoin) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;

		this.isTwoTableJoin = isTwoTableJoin;
		this.type = "equijoinWritePredicate";
	}

	public EquijoinWritePredicate(int table1JoinCol, int table2JoinCol, boolean isTwoTableJoin, String type) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;

		this.isTwoTableJoin = isTwoTableJoin;
		this.type = type;
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

	public boolean getIsTwoTableJoin() {
		return this.isTwoTableJoin;
	}
	
	@Override
	String getType() {
		return this.type;
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
