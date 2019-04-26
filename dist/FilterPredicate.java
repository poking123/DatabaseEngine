import java.util.List;

public class FilterPredicate extends Predicate {
	private List<int[]> predicates;

	private String type;
	private String tableName;
	private int[] columnsToKeep;
	
	public FilterPredicate(List<int[]> predicates, String tableName, int[] columnsToKeep) {
		this.predicates = predicates;
		this.type = "filterPredicate";
		this.tableName = tableName;
		this.columnsToKeep = columnsToKeep;
	}

	public int[] getColumnsToKeep() {
		return this.columnsToKeep;
	}

	public String getTableName() {
		return this.tableName;
	}

	public String toString() {
		return this.type;
	}
	
	@Override
	String getType() {
		return this.type;
	}
	
	public boolean test(int[] row) {
		for (int[] predData : predicates) {
			int column = predData[0];
			int operator = predData[1];
			int compareValue = predData[2];
			
			int rowValue = row[column];
			
			switch (operator) {
				case 0: // equals
					if (rowValue != compareValue) return false;
					break;
				
				case 1: // <
					if (rowValue >= compareValue) return false;
					break;
					
				case 2: // >
					if (rowValue <= compareValue) return false;
					break;
			}
		}
		
		return true;
	}

	
}
