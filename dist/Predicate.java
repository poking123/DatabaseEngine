import java.util.List;

public class Predicate {
	private List<int[]> predicates;
	
	public Predicate(List<int[]> predicates) {
		this.predicates = predicates;
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
