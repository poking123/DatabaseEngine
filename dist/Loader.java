import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.nio.CharBuffer;

public class Loader {

	private Catalog catalog;

	public Loader() {
		catalog = new Catalog();
	}

	public Catalog getCatalog() {
		return this.catalog;
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
		// Adds in a header to the data
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < numOfCols - 1; i++) {
			sb.append(tableName + ".c" + i + ",");
		}
		sb.append(tableName + ".c" + (numOfCols - 1) + ",");
		dos.writeChars(sb.toString());
		
		CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
		CharBuffer cb2 = CharBuffer.allocate(4 * 1024);

		// FileReader reads through the CharBuffer
		while (fr.read(cb1) != -1) {
			cb1.flip();
			int lastNumberStart = 0;
			int index = 0;

			for (int i = 0; i < cb1.length(); i++) {
				if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
                    int numRead = Integer.parseInt(sb.toString());
                    dos.writeInt(numRead);
					lastNumberStart = i + 1;
					// Clears the StringBuilder
					sb.setLength(0);

					// Metadata
					int minValue = minArray[index];
					minArray[index] = numRead < minValue ? numRead : minValue;
					int maxValue = maxArray[index];
					maxArray[index] = numRead < maxValue ? numRead : maxValue;
					
					uniqueSetArray.get(index).add(numRead);

					index = (index + 1) % numOfCols;
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

		// Puts in the number of unique values into an array
		int[] uniqueArray = new int[numOfCols];
		for (int i = 0; i < numOfCols; i++) uniqueArray[i] = uniqueSetArray.get(i).size();

		// Adds the meta data to TableMetaData
		// Then add TableMetaData to the catalog
		TableMetaData tmd = new TableMetaData(numOfCols);
		tmd.setMin(minArray);
		tmd.setMax(maxArray);
		tmd.setUnique(uniqueArray);
		catalog.addData(tableName, tmd);


		// Reading Data Example
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream("tmp.dat")));

		try {
			while (true) {
				System.out.println(dis.readInt());
			}
		} catch (EOFException e) {
			System.out.println("DONE");
		}
		dis.close();

	}
	
	// returns the String of CSV files
	public String getCSVFiles() {
		// Get the CSV files
		Scanner scanner = new Scanner(System.in);
		return scanner.nextLine();
	}
}