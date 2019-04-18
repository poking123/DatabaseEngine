
import java.util.Scanner;

public class Parser {

	private String selectColumnNames;
	private StringBuilder fromData;
	private WhereData whereData;
	private AndData andData;

	public Parser() {
		selectColumnNames = "";
		fromData = new StringBuilder();
		whereData = new WhereData();
		andData = new AndData();
	}

	public void empty() {
		selectColumnNames = "";
		fromData = new StringBuilder();
		whereData = new WhereData();
		andData = new AndData();
	}
	
	public String getSelectColumnNames() {
		return this.selectColumnNames;
	}
	
	public String getFromData() {
		return this.fromData.toString();
	}
	
	public WhereData getWhereData() {
		return this.whereData;
	}
	
	public AndData getAndData() {
		return this.andData;
	}
	
	public void addSelectData(String selectLine) {
		StringBuilder header = new StringBuilder();
		int lastOpenParentheses = -1;
		for (int i = 0; i < selectLine.length(); i++) {
			char c = selectLine.charAt(i);
			if (c == '(') {
				lastOpenParentheses = i;
			} else if (c == ')') {
				header.append(selectLine.substring(lastOpenParentheses + 1, i));
				if (i != selectLine.length() - 1) {
					header.append(",");
				}
			}
		}
		selectColumnNames = header.toString();
	}

	public void addFromData(StringBuilder fromData, String fromLine) {
		for (int i = 0; i < fromLine.length(); i++) {
			char c = fromLine.charAt(i);
			if (c == ' ') {
				char tableName = fromLine.charAt(i + 1);
				fromData.append(tableName);
			}
		}
	}

	public void readQuery(Scanner queryScanner) {
		// Get SELECT line
		// Figure out which column from which table we are summing (looking for)
		// Saves the table and column name (in the right order)
		
		String selectLine = queryScanner.nextLine();
		
		addSelectData(selectLine);

		
		// Get FROM line
		String fromLine = queryScanner.nextLine();
		
		addFromData(fromData, fromLine);

		
		// Get WHERE line
		String whereLine = queryScanner.nextLine();
		
		whereData.addWhereData(whereLine);
		
		
		// Get AND line
		String andLine = queryScanner.nextLine();
		andLine = andLine.substring(0, andLine.length() - 1); // removes the semicolon at the end
		
		andData.addAndData(andLine);
	}

	public int getNumOfQueries(Scanner queryScanner) {
		return 30;
		// return Integer.parseInt(queryScanner.nextLine());
		//return queryScanner.nextInt();
	}
}