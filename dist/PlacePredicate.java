

public class PlacePredicate extends Predicate {
	private String type;
	
	public PlacePredicate() {
		this.type = "placePredicate";
	}

	public String toString() {
		return this.type;
	}
	
	@Override
	String getType() {
		return this.type;
	}
}
