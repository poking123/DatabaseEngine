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

		
		// PARSER
		Parser parser = new Parser();

		

		ExecutionEngine executionEngine = new ExecutionEngine();
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
		
		
		Catalog catalog = loader.getCatalog();
		
		
		
		//int count = 0;
		//Runtime runtime = Runtime.getRuntime();
		//for (int i = 0; i < 10000000; i++) {
		//	count++;
		//	System.out.println(runtime.totalMemory() - runtime.freeMemory());
		//}
	}
}

	