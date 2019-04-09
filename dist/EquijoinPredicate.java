import java.util.List;

public class EquijoinPredicate {

	private int table1JoinCol;
	private int table2JoinCol;
	
	public EquijoinPredicate(int table1JoinCol, int table2JoinCol) {
		this.table1JoinCol = table1JoinCol;
		this.table2JoinCol = table2JoinCol;
	}
	
	public boolean test(int[] table1Row, int[] table2Row) {
		return table1Row[table1JoinCol] == table2Row[table2JoinCol];
	}
}
