
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.nio.CharBuffer;

public class Loader {

	public Loader() {
	}

	public void readAllCSVFiles(String allFiles) throws FileNotFoundException, IOException {
		String[] allFilesArr = allFiles.split(",");
		for (String s : allFilesArr) {
			readCSVFile(s);
		}
	}

	public void readCSVFile(String path) throws FileNotFoundException, IOException {
		int tableNameIndex = path.lastIndexOf('\\') + 1;
		String tableName = path.substring(tableNameIndex, tableNameIndex + 1);

		Scanner scanner = new Scanner(new File(path));
		// Gets the first line for the number of columns
		// And to put values into the metadata arrays
		String[] firstLine = scanner.nextLine().split(",");
		scanner.close();
		int numOfCols = firstLine.length;

		// Arrays for metadata
		int[] minArray = new int[numOfCols];
		int[] maxArray = new int[numOfCols];
		ArrayList<HashSet<Integer>> uniqueSetArray = new ArrayList<HashSet<Integer>>();
		for (int i = 0; i < numOfCols; i++) uniqueSetArray.add(new HashSet<Integer>());

		// Places actual values in the metadata arrays
		for (int i = 0; i < numOfCols; i++) {
			int temp = Integer.parseInt(firstLine[i]);
			minArray[i] = temp;
			minArray[i] = temp;
		}


		FileReader fr = new FileReader(path);
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName + ".dat")));
		
		StringBuilder sb = new StringBuilder();
		
		
		CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
		CharBuffer cb2 = CharBuffer.allocate(4 * 1024);

		// Keeps track of the index
		// And use this to get the total number of rows
		int index = 0;
		// FileReader reads through the CharBuffer
		while (fr.read(cb1) != -1) {
			cb1.flip();
			int lastNumberStart = 0;

			for (int i = 0; i < cb1.length(); i++) {
				if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
                    int numRead = Integer.parseInt(sb.toString());
                    dos.writeInt(numRead);
					lastNumberStart = i + 1;
					// Clears the StringBuilder
					sb.setLength(0);

					// Metadata
					int modIndex = index % numOfCols;
					
					int minValue = minArray[modIndex];
					minArray[modIndex] = numRead < minValue ? numRead : minValue;
					int maxValue = maxArray[modIndex];
					maxArray[modIndex] = numRead < maxValue ? numRead : maxValue;
					
					uniqueSetArray.get(modIndex).add(numRead);

					index++;
				} else {
					sb.append(cb1.charAt(i));
				}
			}

			// Clears the StringBuilder
			sb.setLength(0);
			// Clears the CharBuffer
			cb2.clear();
			// Transfers the contents of the CharBuffer
            cb2.append(cb1, lastNumberStart, cb1.length());

            CharBuffer temp = cb2;
            cb2 = cb1;
            cb1 = temp;
		}

		fr.close();
		dos.close();

		// METADATA
		
		// Puts in the number of unique values into an array
		int[] uniqueArray = new int[numOfCols];
		for (int i = 0; i < numOfCols; i++) uniqueArray[i] = uniqueSetArray.get(i).size();

		
		// Makes header with all the column names
		for (int i = 0; i < numOfCols - 1; i++) {
			sb.append(tableName + ".c" + i + ",");
		}
		sb.append(tableName + ".c" + (numOfCols - 1));
		String columnNames = sb.toString();
		
		// Adds the meta data to TableMetaData
		// Then add TableMetaData to the catalog
		TableMetaData tmd = new TableMetaData(columnNames);
		tmd.setRows(index / numOfCols);
		tmd.setColumns(numOfCols);
		tmd.setMin(minArray);
		tmd.setMax(maxArray);
		tmd.setUnique(uniqueArray);
		Catalog.addData(tableName + ".dat", tmd);
	}
	
	// returns the String of CSV files
	public String getCSVFiles() {
		// Get the CSV files
		Scanner scanner = new Scanner(System.in);
		scanner.close();
		return scanner.nextLine();
	}
}