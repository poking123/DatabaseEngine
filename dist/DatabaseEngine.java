import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;


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
		// Scan scanA = new Scan("A.dat");
		// Scan scanB = new Scan("B.dat");

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

		//System.exit(0);
		////////////////////////////////////////
		
		// PARSER
		Parser parser = new Parser();

		// Optimizer
		Optimizer optimizer = new Optimizer();
		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();

		///////////////////////////////////////////////////////////////////

		// Representation of the first query from the xxxs dataset
		Queue<Queue<RAOperation>> tablesQueue = new LinkedList<>();
		Queue<Queue<Predicate>> predicatesQueue = new LinkedList<>();


		Queue<RAOperation> tableQueue = new LinkedList<>();
		RAOperation scanA = new Scan("A.dat");
		RAOperation scanC = new Scan("C.dat");
		RAOperation scanB = new Scan("B.dat");
		RAOperation scanD = new Scan("D.dat");

		tableQueue.add(scanA);
		tableQueue.add(scanB);
		tableQueue.add(scanD);
		// tableQueue.add(scanC);

		Queue<Predicate> predicateQueue = new LinkedList<>();

		Predicate abE = new EquijoinPredicate(1, 0, true);
		Predicate dE = new EquijoinPredicate(3, 0, true);
		

		ArrayList<int[]> DList = new ArrayList<>();
		int[] dPred = {3, 0, -9496};
		DList.add(dPred);
		Predicate sigmaD = new FilterPredicate(DList);
		
		Predicate dC = new EquijoinPredicate(15, 2, true);
		
		predicateQueue.add(abE);
		predicateQueue.add(sigmaD);
		predicateQueue.add(dE);
		// predicateQueue.add(dC);

		tablesQueue.add(tableQueue);
		predicatesQueue.add(predicateQueue);

		Queue<Predicate> finalPredicateQueue = new LinkedList<>();
		//columns to sum
		// 15,19,21
		// int[] colsToSum = {15, 19, 21};
		executionEngine.executeQuery(tablesQueue, predicatesQueue, finalPredicateQueue, new int[]{0, 1, 2});
		System.exit(0);
		///////////////////////////////////////////////////////////////////

		//Scanner queryScanner = new Scanner(System.in);
		Scanner queryScanner = new Scanner(new File("../../data/xxxs\\queryTest.sql"));

		// Gets the number of queries
		int numOfQueries = parser.getNumOfQueries(queryScanner);
		
		// Read each query
		for (int i = 0; i < numOfQueries - 1; i++) {
			parser.readQuery(queryScanner);
			queryScanner.nextLine(); // Blank Line
			
			optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
			
			// Execute Query
			executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum());

			// Gets rid of all the data in the parser
			parser.empty();
		}
		// Last Query
		parser.readQuery(queryScanner);

		optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
		
		// Execute Query
		executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum());
		
	}
}

	