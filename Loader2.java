import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.PrintStream;

public class Loader {
	
	// returns the String of CSV files
	public String getCSVFiles() {
		// Get the CSV files
		Scanner scanner = new Scanner(System.in);
		return scanner.nextLine();
	}
	
	// Reads the contents of the CSV files and writes them to txt files
	public static void readCSVFiles(String CSVFiles) throws FileNotFoundException {
		// String CSVFiles is a string of file names separated by a comma
		
		String[] fileNames = CSVFiles.split(",");
		for (int i = 0; i < fileNames.length; i++) {
			// Read the CSV files
			String fileName = fileNames[i];
			String tableName = fileName.substring(fileName.length() - 5, fileName.length() - 4);
			String tableLetter = tableName.substring(0, 1);
			
			File f = new File(fileName);
			Scanner readCSV = new Scanner(f);
			readCSV.useDelimiter(",");
			
			PrintStream ps = new PrintStream(new File(tableName + ".txt"));
			
			// Reads the first line and adds the header
			String firstLine = readCSV.nextLine();
			String[] firstLineArr = firstLine.split(",");
			for (int j = 0; j < firstLineArr.length - 1; j++) {
				ps.print(tableLetter + ".c" + j + ",");
			}
			ps.println(tableLetter + ".c" + (firstLineArr.length - 1));
			firstLine = firstLine.replaceAll(",", " ");
			ps.println(firstLine);
			
			// Reads and stores the rest of the data
			// Goes through each line of the data
			while(readCSV.hasNext()){
				String line = readCSV.nextLine();
				line = line.replaceAll(",", " "); // Separates each value by a space
				ps.println(line);
			}
		}
	}
	
	
	
}