import java.util.ArrayList;

public class PredicateChecker {
	
	private ArrayList<Integer> operators;
	private ArrayList<Integer> values;
	
	public void addPredicate(int operator, int value) {
		operators.add(operator);
		values.add(value);
	}
	
	public boolean checkPredicates(int rowValue) {
		for (int i = 0; i < operators.size(); i++) {
			switch (operators.get(i)) {
			case 0:
				if (rowValue != values.get(i)) return false;
				break;
			
			case 1:
				if (rowValue >= values.get(i)) return false;
				break;
				
			case 2:
				if (rowValue <= values.get(i)) return false;
				break;
			}
		}
		return true;
	}
	
}
