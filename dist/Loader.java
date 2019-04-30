import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;
import java.util.TreeMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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
		int tableNameIndex = path.lastIndexOf('/') + 1;
		// String tableName = path.substring(tableNameIndex, tableNameIndex + 1);
		char tableName = path.charAt(tableNameIndex);

		Scanner scanner = new Scanner(new File(path));
		// Gets the first line for the number of columns
		// And to put values into the metadata arrays
		String[] firstLine = scanner.nextLine().split(",");
		scanner.close();
		int numOfCols = firstLine.length;

		// Arrays for metadata
		int[] minArray = new int[numOfCols];
		int[] maxArray = new int[numOfCols];
		// ArrayList<HashSet<Integer>> uniqueSetArray = new ArrayList<HashSet<Integer>>();
		// for (int i = 0; i < numOfCols; i++) uniqueSetArray.add(new HashSet<Integer>());

		// Places actual values in the metadata arrays
		for (int i = 0; i < numOfCols; i++) {
			int temp = Integer.parseInt(firstLine[i]);
			minArray[i] = temp;
			maxArray[i] = temp;
		}


		FileReader fr = new FileReader(path);
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tableName + ".dat")));
		
		
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
					int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);
                    dos.writeInt(numRead);
					lastNumberStart = i + 1;

					// Metadata
					int modIndex = index % numOfCols;
					
					int minValue = minArray[modIndex];
					minArray[modIndex] = numRead < minValue ? numRead : minValue;
					int maxValue = maxArray[modIndex];
					maxArray[modIndex] = numRead > maxValue ? numRead : maxValue;
					
					//// uniqueSetArray.get(modIndex).add(numRead);

					index++;
				}
			}

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

		int numOfRows = index / numOfCols;
		
		// Puts in the number of unique values into an array
		int[] uniqueArray = new int[numOfCols];
		//// for (int i = 0; i < numOfCols; i++) uniqueArray[i] = uniqueSetArray.get(i).size();

		// estimate for number of unique values in a column is the min of the number of columns or the max - min
		for (int i = 0; i < numOfCols; i++) {
			int maxMinDiff = maxArray[i] - minArray[i];
			uniqueArray[i] = (numOfRows < maxMinDiff) ? numOfRows : maxMinDiff;
		}

		StringBuilder sb = new StringBuilder();
		
		// // Makes header with all the column names
		for (int i = 0; i < numOfCols - 1; i++) {
			sb.append(tableName + ".c" + i + ",");
		}
		sb.append(tableName + ".c" + (numOfCols - 1));
		String columnNames = sb.toString();
		
		// Adds the meta data to TableMetaData
		// Then add TableMetaData to the catalog
		TableMetaData tmd = new TableMetaData(columnNames);
		tmd.setRows(numOfRows);
		tmd.setColumns(numOfCols);
		tmd.setMin(minArray);
		tmd.setMax(maxArray);
		tmd.setUnique(uniqueArray);
		Catalog.addData(tableName + ".dat", tmd);
		


		// for (int i = 0; i < numOfCols; i++) {
		// 	int tableJoinCol = i;
		// 	// System.out.println("tableJoinCol is " + i);
		// 	// System.out.println("num of unique is " + uniqueArray[tableJoinCol]);
		// 	// System.out.println("num of rows is " + numOfRows);
		// 	// System.out.println();
		// 	if (uniqueArray[tableJoinCol] == numOfRows || uniqueArray[tableJoinCol] == (numOfRows - 1)) {
		// 		String sortedFileName = writeSortedFileToDisk(tableName, numOfCols, tableJoinCol, dos);
		// 		DatabaseEngine.sortedColumnsMap.put(tableName + "" + tableJoinCol + ".dat", sortedFileName);
		// 	}
		// }
		


		// for debugging, to read the data
		// String fileNameToRead = DatabaseEngine.sortedColumnsMap.get(tableName + "" + tableJoinCol + ".dat");

		// HashSet<Integer> colsToKeepSet = new HashSet<>();
		// colsToKeepSet.add(1);
		// colsToKeepSet.add(4);

		// int colsToKeepSize = colsToKeepSet.size();

		// int rowBufferSize = 2;
		// DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tableName + ".dat"))); 
		
		// ByteBuffer bb = ByteBuffer.allocate(1024 * 8);
		// bb.flip();
		
		// while (numOfRows > 0) {
		// 	index = 0;	
		// 	int oldColIndex = 0;

		// 	// int[] oldRow = new int[numOfCols];
		// 	// while (index < numOfCols && bb.hasRemaining() && numOfRows > 0) {
		// 	// 	int value = bb.getInt();
		// 	// 	oldRow[index] = value;
		// 	// 	index = (index + 1) % numOfCols;
		// 	// 	if (index == 0) {
		// 	// 		numOfRows--;
		// 	// 	}
		// 	// }

		// 	int[] oldRow = new int[colsToKeepSize];
		// 	while (index < numOfCols && bb.hasRemaining() && numOfRows > 0) {
		// 		int value = bb.getInt();
		// 		if (colsToKeepSet.contains(index)) {
		// 			oldRow[oldColIndex] = value;
		// 			oldColIndex++;
		// 		}
		// 		index = (index + 1) % numOfCols;
		// 		if (index == 0) {
		// 			oldColIndex = 0;
		// 			print(oldRow);
		// 			numOfRows--;
		// 		}
		// 	}
			

		// 	boolean finishRow = (index != 0);

		// 	byte[] buffer = new byte[4 * 1024];
		// 	int bytesRead = dis.read(buffer);
		// 	bb = ByteBuffer.wrap(buffer);

		// 	if (finishRow) {
		// 		while (index < numOfCols) {
		// 			int value = bb.getInt();
		// 			if (colsToKeepSet.contains(index)) {
		// 				oldRow[oldColIndex] = value;
		// 				oldColIndex++;
		// 			}
		// 			index++;
		// 		}
		// 		print(oldRow);
		// 		numOfRows--;
		// 	}
			

		// RandomAccessFile raf = new RandomAccessFile(new File(tableName + ".dat"), "r");
		// //Get file channel in read-only mode
		// FileChannel fileChannel = raf.getChannel();
             
		// //Get direct byte buffer access using channel.map() operation
		// MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

		// while (buffer.hasRemaining()) {
		// 	for (int i = 0; i < numOfCols; i++) {
		// 		System.out.print(buffer.getInt() + " ");
		// 	}
		// 	System.out.println();
		// }


		// System.exit(0);





			// byte[] buffer = new byte[1024 * 4];

			// int bytesRead = dis.read(buffer, 0, 4 * numOfCols * rowBufferSize);

			// // for each integer, put it in the intbuffer
			// for (int j = 0; j < bytesRead / 4; j += numOfCols) {
			// 	for (int i = 0; i < numOfCols; i++) {
			// 		byte[] newByteArr = Arrays.copyOfRange(buffer, 4 * i + 4 * j, 4 * i + 4 + 4 * j);
			// 		int value = fromByteArray(newByteArr);
			// 		arrayOfIntegers[intArrIndex] = value;
			// 		intArrIndex = (intArrIndex + 1) % arrayOfIntegers.length;
			// 	}

			// }
		// }
		// System.exit(0);
		// for (int i = 0; i < 2; i++) {
			
		// 	byte[] buffer = new byte[4];

		// 	dis.read(buffer, 0, 4);
		// 	System.out.println(buffer[0]);
		// 	System.out.println(buffer[1]);
		// 	System.out.println(buffer[2]);
		// 	System.out.println(buffer[3]);

		// 	System.out.println();
		// 	System.out.println(byteArrayToInt(buffer));
		// }

		// System.out.println(bytesToInt((byte) 0, (byte) 0, (byte) 8, (byte) 128));

	}

	
	// returns the String of CSV files
	public String getCSVFiles(Scanner scanner) {
		// Get the CSV files
		return scanner.nextLine();
	}

	public String writeSortedFileToDisk(char tableName, int numOfCols, int tableJoinCol, DataOutputStream dos) throws IOException {
		// rewrite sorted columns
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tableName + ".dat")));
		
		Queue<String> holder1 = new LinkedList<>();
		Queue<String> holder2 = new LinkedList<>();


		while (dis.available() != 0) {
			int tempNumber = DatabaseEngine.tempNumber;
			DatabaseEngine.tempNumber++;
			dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
			holder1.add(tempNumber + ".dat");

			// gets buffer of rows
			Queue<int[]> tableRows = new LinkedList<>();
			while (tableRows.size() < DatabaseEngine.bufferSize && dis.available() != 0) {
				int[] row = new int[numOfCols];
				for (int i = 0; i < numOfCols; i++) {
					row[i] = dis.readInt();
				}
				tableRows.add(row);
			}

			TreeMap<Integer, Queue<int[]>> columnValueToRowsMap = new TreeMap<>();
			while (!tableRows.isEmpty()) { // writes all rows to map
				Queue<int[]> valueRows = new LinkedList<>();
				int[] tableRow = tableRows.remove();
				int tableJoinColValue = tableRow[tableJoinCol];
				if (columnValueToRowsMap.containsKey(tableJoinColValue)) {
					valueRows = columnValueToRowsMap.get(tableJoinColValue);
				}
				valueRows.add(tableRow);
				columnValueToRowsMap.put(tableJoinColValue, valueRows);
			}
			
			// writes the sorted keyset
			while (!columnValueToRowsMap.isEmpty()) {
				int key = columnValueToRowsMap.firstKey();
				Queue<int[]> tableSortedRows = columnValueToRowsMap.get(key);
				while (!tableSortedRows.isEmpty()) {
					int[] tableSortedRow = tableSortedRows.remove();
					for (int i : tableSortedRow) {
						dos.writeInt(i);
					}
				}
				columnValueToRowsMap.remove(key);
			}
			dos.close();
		}
		dis.close();

		while (holder1.size() != 1) {
			while (!holder1.isEmpty()) {
				if (holder1.size() == 1) {
					holder2.add(holder1.remove());
				} else {
					mergeFiles(holder1.remove(), holder1.remove(), numOfCols, tableJoinCol, holder1, holder2);
				}
			}
			
			holder1 = holder2;
			holder2 = new LinkedList<>();
		}
		
		return holder1.remove();
	}

	public void mergeFiles(String table1, String table2, int tableCols, int tableJoinCol, Queue<String> holder1, Queue<String> holder2) throws IOException {
		
		int tempNumber = DatabaseEngine.tempNumber;
		DatabaseEngine.tempNumber++;
		DataOutputStream tempDOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
		
		holder2.add(tempNumber + ".dat");
		
		DataInputStream dis1 = Catalog.openStream(table1);
		DataInputStream dis2 = Catalog.openStream(table2);
		
		// fills the rows for both tables
		int[] table1TempRow = new int[tableCols];
		int[] table2TempRow = new int[tableCols];
		
		boolean writeTable1 = true;
		boolean writeTable2 = true;

		boolean table1Done = false;
		boolean table2Done = false;
		while (!table1Done && !table2Done) {

			if (writeTable1) {
				for (int i = 0; i < tableCols; i++) {
					int value = dis1.readInt();
					table1TempRow[i] = value;
				}
				writeTable1 = false;
			}
			
			if (writeTable2) {
				for (int i = 0; i < tableCols; i++) {
					int value = dis2.readInt();
					table2TempRow[i] = value;
				}
				writeTable2 = false;
			}
			
			
			int table1JoinColValue = table1TempRow[tableJoinCol];
			int table2JoinColValue = table2TempRow[tableJoinCol];
			
			// write the smaller row
			if (table1JoinColValue < table2JoinColValue) {
				
				for (int i : table1TempRow) {
					tempDOS.writeInt(i);
				}
				writeTable1 = true;
				if (dis1.available() == 0) table1Done = true;
				
			} else {
				for (int i : table2TempRow) {
					tempDOS.writeInt(i);
				}
				writeTable2 = true;
				if (dis2.available() == 0) table2Done = true;
			}
		}
		
		if (table1Done) {
			for (int i : table2TempRow) { // write what's currently in table2Row
				tempDOS.writeInt(i);
			}

			while (dis2.available() != 0) { // write what's left in dis2
				for (int i = 0; i < tableCols; i++) {
					int value = dis2.readInt();
					tempDOS.writeInt(value);
				}
			}
		} else if (table2Done) {
			for (int i : table1TempRow) { // write what's currently in table1Row
				tempDOS.writeInt(i);
			}

			while (dis1.available() != 0) { // write what's left in dis1
				for (int i = 0; i < tableCols; i++) {
					int value = dis1.readInt();
					tempDOS.writeInt(value);
				}
			}
		}
		
		tempDOS.close();
	}
 }