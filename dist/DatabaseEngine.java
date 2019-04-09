import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;


public class DatabaseEngine {

	public static void main(String[] args) throws IOException {
		
		// LOADER
		Loader loader = new Loader();
		
		// Get the CSV files
		//String CSVFiles = loader.getCSVFiles();
		String CSVFiles = "../../data/xxxs\\B.csv,../../data/xxxs\\C.csv,../../data/xxxs\\A.csv,../../data/xxxs\\D.csv,../../data/xxxs\\E.csv";
		//String CSVFiles = "../../data/xxs\\B.csv,../../data/xxs\\C.csv,../../data/xxs\\A.csv,../../data/xxs\\D.csv,../../data/xxs\\E.csv,../../data/xxs\\F.csv";
		//String CSVFiles = "../../data/xs\\B.csv,../../data/xs\\C.csv,../../data/xs\\A.csv,../../data/xs\\D.csv,../../data/xs\\E.csv,../../data/xs\\F.csv";

		// Loader loads all the data into storage
		loader.readAllCSVFiles(CSVFiles);
		
		
		//////////////////////////////////
		Scan scanA = new Scan("A.dat");
		Scan scanB = new Scan("B.dat");

		// EquijoinPredicate ep = new EquijoinPredicate(1, 0);
		// Equijoin equijoin = new Equijoin(scanA, scanB, ep);
		

		// Iterator<List<int[]>> ejItr = equijoin.iterator();

		// while (ejItr.hasNext()) {
		// 	List<int[]> rowBlock = ejItr.next();
		// 	for (int[] row : rowBlock) {
		// 		for (int i : row) {
		// 			System.out.print(i + " ");
		// 		}
		// 		System.out.println();
		// 	}
		// }
		
		// List<int[]> predList = new ArrayList<>();
		// int[] data = {1, 1, 0};
		// predList.add(data);
		// Predicate predicate = new Predicate(predList);
		// Filter filter = new Filter(scan, predicate);

		// Iterator<List<int[]>> scanItr = scan.iterator();
		// while (scanItr.hasNext()) {
		// 	List<int[]> rows = scanItr.next();
		// 	for (int[] row : rows) {
		// 		for (int i : row) {
		// 			System.out.print(i + " ");
		// 		}
		// 		System.out.println();
		// 	}
		// }
		
//		Iterator<List<int[]>> filterItr = filter.iterator();
//		
//		while (filterItr.hasNext()) {
//			List<int[]> rows = filterItr.next();
//			for (int[] row : rows) {
//				for (int i : row) {
//					System.out.print(i + " ");
//				}
//				System.out.println();
//			}
//		}

		System.exit(0);
		////////////////////////////////////////
		
		// PARSER
		Parser parser = new Parser();

		// Optimizer
		Optimizer optimizer = new Optimizer();
		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();

		//Scanner queryScanner = new Scanner(System.in);
		Scanner queryScanner = new Scanner(new File("../../data/xs\\queries.sql"));

		// Gets the number of queries
		int numOfQueries = parser.getNumOfQueries(queryScanner);
		
		// Read each query
		for (int i = 0; i < numOfQueries - 1; i++) {
			parser.readQuery(queryScanner);
			queryScanner.nextLine(); // Blank Line
			
			optimizer.optimizeQuery(parser.getFromData(), parser.getWhereData(), parser.getAndData());
			
			HashMap<Character, ArrayList<int[]>> tablePredicateMap = parser.getAndData().getTablePredicateMap(); // tableName -> predicate data
			
			// Execute Query
			executionEngine.executeQuery(parser.getSelectColumnNames(), tablePredicateMap, optimizer.getPredicateJoinQueueMap(), optimizer.getDisjointDeque());

			// Gets rid of all the data in the parser
			parser.empty();
		}
		// Last Query
		parser.readQuery(queryScanner);

		optimizer.optimizeQuery(parser.getFromData(), parser.getWhereData(), parser.getAndData());
			
		HashMap<Character, ArrayList<int[]>> tablePredicateMap = parser.getAndData().getTablePredicateMap(); // tableName -> predicate data
		
		// Execute Query
		executionEngine.executeQuery(parser.getSelectColumnNames(), tablePredicateMap, optimizer.getPredicateJoinQueueMap(), optimizer.getDisjointDeque());
		
	}
}

	