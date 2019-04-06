import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Queue;
import java.util.LinkedList;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class DatabaseEngine {
	public static void main(String[] args) throws IOException {
		
		// LOADER
		Loader loader = new Loader();
		
		// Get the CSV files
		//String CSVFiles = loader.getCSVFiles();
		String CSVFiles = "../../data/xxxs\\B.csv,../../data/xxxs\\C.csv,../../data/xxxs\\A.csv,../../data/xxxs\\D.csv,../../data/xxxs\\E.csv";
		
		// Loader loads all the data into storage
		loader.readAllCSVFiles(CSVFiles);

		Catalog catalog = loader.getCatalog();

		//////////////////////////////////////
		ExecutionEngine executionEngine2 = new ExecutionEngine();
		// Sets the execution engine's catalog
		executionEngine2.setCatalog(catalog);

		executionEngine2.equiJoinBNLJ("A.dat", "A.c1", "B.dat", "B.c0");
		System.exit(0);
		///////////////////////////////////////

		
		// PARSER
		Parser parser = new Parser();

		
		// EXECUTION ENGINE
		ExecutionEngine executionEngine = new ExecutionEngine();
		// Sets the execution engine's catalog
		executionEngine.setCatalog(catalog);
		
		Scanner queryScanner = new Scanner(System.in);

		// Gets the number of queries
		int numOfQueries = parser.getNumOfQueries(queryScanner);

		for (int i = 0; i < numOfQueries - 1; i++) {
			parser.readQuery(queryScanner);
			queryScanner.nextLine(); // Blank Line
			
			// Execute Query
			executionEngine.executeQuery(parser.getColumnsQueue(), parser.getFromColumns(), parser.getWhereColumns(), parser.getAndColumns());
		}
		// Last Query
		parser.readQuery(queryScanner);
		

		
		
		
		//int count = 0;
		//Runtime runtime = Runtime.getRuntime();
		//for (int i = 0; i < 10000000; i++) {
		//	count++;
		//	System.out.println(runtime.totalMemory() - runtime.freeMemory());
		//}
	}
}

	