import java.io.File;
import java.io.IOException;
import java.util.Scanner;


public class DatabaseEngine {
	
	static final int bufferSize = 2000000;
	static int tempNumber = 0;
	// static int byteBufferSize = 1024 * 2;
	// static int dataInputBufferSize = 1024 * 1;
	// static final int scanBufferSize = 1024;

	// static HashMap<String, String> sortedColumnsMap = new HashMap<>();

	// static int finalNumber = 0;
	////////
	public static void main(String[] args) throws IOException {
		// Make Empty Scan
		TableMetaData tmd = new TableMetaData("");
		tmd.setRows(0);
		Catalog.addData("Empty.dat", tmd);

		File f = new File("Empty.dat");
		f.createNewFile();
		
		// LOADER
		Loader loader = new Loader();
		
		Scanner scanner = new Scanner(System.in);
		////////
		
		// Get the CSV files
		String CSVFiles = loader.getCSVFiles(scanner);
		// String CSVFiles = "../../data/xxxs/B.csv,../../data/xxxs/C.csv,../../data/xxxs/A.csv,../../data/xxxs/D.csv,../../data/xxxs/E.csv";
		// String CSVFiles = "../../data/xxs/B.csv,../../data/xxs/C.csv,../../data/xxs/A.csv,../../data/xxs/D.csv,../../data/xxs/E.csv,../../data/xxs/F.csv";
		// String CSVFiles = "../../data/xs/B.csv,../../data/xs/C.csv,../../data/xs/A.csv,../../data/xs/D.csv,../../data/xs/E.csv,../../data/xs/F.csv";
		// String CSVFiles = "../../data/s/B.csv,../../data/s/C.csv,../../data/s/A.csv,../../data/s/D.csv,../../data/s/E.csv,../../data/s/F.csv";
		// m size dataset
		// String CSVFiles = "../../data/m/A.csv,../../data/m/B.csv,../../data/m/C.csv,../../data/m/D.csv,../../data/m/E.csv,../../data/m/F.csv,";
		// CSVFiles += "../../data/m/G.csv,../../data/m/H.csv,../../data/m/I.csv,../../data/m/J.csv,../../data/m/K.csv,../../data/m/L.csv,";
		// CSVFiles += "../../data/m/M.csv,../../data/m/N.csv,../../data/m/O.csv,../../data/m/P.csv";
		// l size dataset
		// String CSVFiles = "../../data/l/A.csv,../../data/l/B.csv,../../data/l/C.csv,../../data/l/D.csv,../../data/l/E.csv,../../data/l/F.csv,";
		// CSVFiles += "../../data/l/G.csv,../../data/l/H.csv,../../data/l/I.csv,../../data/l/J.csv,../../data/l/K.csv,../../data/l/L.csv,";
		// CSVFiles += "../../data/l/M.csv,../../data/l/N.csv,../../data/l/O.csv,../../data/l/P.csv,../../data/l/Q.csv";
		// l2 dataset
		// String CSVFiles = "../../data/l2/B.csv,../../data/l2/C.csv,../../data/l2/A.csv,../../data/l2/D.csv,../../data/l2/E.csv,../../data/l2/F.csv";
		
		// Loader loads all the data into storage
		
		loader.readAllCSVFiles(CSVFiles);

		// PARSER
		Parser parser = new Parser();

		// Optimizer
		Optimizer optimizer = new Optimizer();
		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();
		
		// scanner = new Scanner(new File("../../data/xxs/queries.sql"));
		// scanner = new Scanner(new File("../../data/xxs/queryTest.sql"));
		// Gets the number of queries
		int numOfQueries = parser.getNumOfQueries(scanner);

		// Read each query
		for (int i = 0; i < numOfQueries - 1; i++) {
			parser.readQuery(scanner);

			scanner.nextLine(); // Blank Line
			
			optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());

			// Gets rid of all the data in the parser
			parser.empty();

			// Execute Query
			executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum(), optimizer.getswitchesQueue());	
		}
		// Last Query
		parser.readQuery(scanner);

		optimizer.optimizeQuery(parser.getSelectColumnNames(), parser.getFromData(), parser.getWhereData(), parser.getAndData());
		
		// Execute Query
		executionEngine.executeQuery(optimizer.getTablesQueue(), optimizer.getPredicatesQueue(), optimizer.getFinalPredicateQueue() , optimizer.getColumnsToSum(), optimizer.getswitchesQueue());
		
	}
}

	