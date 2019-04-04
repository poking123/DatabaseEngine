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

	public static void readAllCSVFiles(String allFiles) {
		String[] allFilesArr = allFiles.split(",");
		for (String s : allFilesArr) {
			readCSVFile(s);
		}
	}

	public static void readCSVFile(String path) throws FileNotFoundException, IOException {
		FileReader fr = new FileReader(path);
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("tmp.dat")));
		
		CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
		CharBuffer cb2 = CharBuffer.allocate(4 * 1024);

		while (fr.read(cb1) != -1) {
			cb1.flip();
			int lastNumberStart = 0;
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < cb1.length(); i++) {
				if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
                    int numRead = Integer.parseInt(sb.toString());
                    dos.writeInt(numRead);
					lastNumberStart = i + 1;
					sb.setLength(0);
				} else {
					sb.append(cb1.charAt(i));
				}
			}

			sb.setLength(0);
			cb2.clear();
            cb2.append(cb1, lastNumberStart, cb1.length());

            CharBuffer temp = cb2;
            cb2 = cb1;
            cb1 = temp;
		}

		fr.close();
		dos.close();

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