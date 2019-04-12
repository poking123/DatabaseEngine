import java.io.IOException;
import java.util.Scanner;
import java.io.File;


public class DatabaseEngine {

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		// LOADER
		Loader loader = new Loader();
		
		Scanner scanner = new Scanner(System.in);
		
		// Get the CSV files
		// String CSVFiles = loader.getCSVFiles(scanner);
		// String CSVFiles = "../../data/xxxs/B.csv,../../data/xxxs/C.csv,../../data/xxxs/A.csv,../../data/xxxs/D.csv,../../data/xxxs/E.csv";
		// String CSVFiles = "../../data/xxs/B.csv,../../data/xxs/C.csv,../../data/xxs/A.csv,../../data/xxs/D.csv,../../data/xxs/E.csv,../../data/xxs/F.csv";
		// String CSVFiles = "../../data/xs/B.csv,../../data/xs/C.csv,../../data/xs/A.csv,../../data/xs/D.csv,../../data/xs/E.csv,../../data/xs/F.csv";
		// String CSVFiles = "../../data/s/B.csv,../../data/s/C.csv,../../data/s/A.csv,../../data/s/D.csv,../../data/s/E.csv,../../data/s/F.csv";
		// m size dataset
		String CSVFiles = "../../data/m/A.csv,../../data/m/B.csv,../../data/m/C.csv,../../data/m/D.csv,../../data/m/E.csv,../../data/m/F.csv,";
		CSVFiles += "../../data/m/G.csv,../../data/m/H.csv,../../data/m/I.csv,../../data/m/J.csv,../../data/m/K.csv,../../data/m/L.csv,";
		CSVFiles += "../../data/m/M.csv,../../data/m/N.csv,../../data/m/O.csv,../../data/m/P.csv";
		// l size dataset
		// String CSVFiles = "../../data/l/A.csv,../../data/l/B.csv,../../data/l/C.csv,../../data/l/D.csv,../../data/l/E.csv,../../data/l/F.csv,";
		// CSVFiles += "../../data/l/G.csv,../../data/l/H.csv,../../data/l/I.csv,../../data/l/J.csv,../../data/l/K.csv,../../data/l/L.csv,";
		// CSVFiles += "../../data/l/M.csv,../../data/l/N.csv,../../data/l/O.csv,../../data/l/P.csv,../../data/l/Q.csv";
		
		// Loader loads all the data into storage
		loader.readAllCSVFiles(CSVFiles);
		long stop = System.currentTimeMillis();
		System.out.println(stop - start);
		System.out.println();
		
		// PARSER
		Parser parser = new Parser();

		// Optimizer
		Optimizer optimizer = new Optimizer();
		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();

		///////////////////////////////////////////////////////////////////

		// Representation of the first query from the xxs dataset
		// Queue<Queue<RAOperation>> tablesQueue = new LinkedList<>();
		// Queue<Queue<Predicate>> predicatesQueue = new LinkedList<>();


		// Queue<RAOperation> tableQueue = new LinkedList<>();
		// RAOperation scanA = new Scan("A.dat");
		// RAOperation scanB = new Scan("B.dat");
		// RAOperation scanC = new Scan("C.dat");
		// RAOperation scanD = new Scan("D.dat");
		// RAOperation scanE = new Scan("E.dat");
		// RAOperation scanF = new Scan("F.dat");

		
		// // tableQueue.add(scanB);
		
		
		
		// // tableQueue.add(scanC);
		
		// tableQueue.add(scanA);
		// tableQueue.add(scanF);
		// tableQueue.add(scanD);
		// tableQueue.add(scanE);
		

		// Queue<Predicate> predicateQueue = new LinkedList<>();

		// Predicate ad = new EquijoinPredicate(3, 0, true);
		// // Predicate dE = new EquijoinPredicate(3, 0, true);
		

		// ArrayList<int[]> FList = new ArrayList<>();
		// int[] fPred = {3, 1, 0};
		// FList.add(fPred);
		// Predicate sigmaF = new FilterPredicate(FList);
		
		// Predicate fd = new EquijoinPredicate(0, 1, true);

		// ArrayList<int[]> EList = new ArrayList<>();
		// int[] ePred = {3, 1, 0};
		// EList.add(ePred);
		// Predicate sigmaE = new FilterPredicate(EList);


		// // Predicate c = new EquijoinPredicate(5, 1, true);
		// Predicate d = new EquijoinPredicate(56, 1, true);
		// // Predicate a = new EquijoinPredicate(10, 2, true);
		
		// predicateQueue.add(ad);
		// predicateQueue.add(sigmaF);
		// predicateQueue.add(fd);
		// predicateQueue.add(sigmaE);
		// predicateQueue.add(d);
		// // predicateQueue.add(a);

		// tablesQueue.add(tableQueue);
		// predicatesQueue.add(predicateQueue);

		// Queue<Predicate> finalPredicateQueue = new LinkedList<>();

		// int[] colsToSum = {61, 63, 55};
		// int[] colsToSum = {0, 1, 2};
		// executionEngine.executeQuery(tablesQueue, predicatesQueue, finalPredicateQueue, colsToSum);
		// System.exit(0);
		///////////////////////////////////////////////////////////////////

		//Scanner queryScanner = new Scanner(System.in);
		
		scanner = new Scanner(new File("../../data/s/queries.sql"));

		// Gets the number of queries
		int numOfQueries = parser.getNumOfQueries(scanner);


		// Read each query
		for (int i = 0; i < numOfQueries - 1; i++) {
			parser.readQuery(scanner);
			scanner.nextLine(); // Blank Line
			
			optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
			
			// Execute Query
			executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum());

			// Gets rid of all the data in the parser
			parser.empty();
		}
		// Last Query
		parser.readQuery(scanner);

		optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
		
		// Execute Query
		executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum());
		
	}
}

	