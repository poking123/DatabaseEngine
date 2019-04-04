import java.io.DataOutputStream;
import java.nio.CharBuffer;
import java.io.*;

public class ClassExample {
    public static void main(String[] args) throws IOException, InterruptedException {
        String path = "C:/Users/sale/Desktop/Spring_2019/COSI127B/PA3/data/xxxs/A.csv";
        FileReader fr = new FileReader(path);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("tmp.dat")));

        long start = System.currentTimeMillis();

        //String line = fr.readLine();

        // while (line != null) {
        //     int startOfNumber = 0;
        //     for (int i = 0; i < line.length(); i++) {
        //         if (line.charAt(i) == ',') {
        //             int toWrite = Integer.parseInt(startOfNumber, i, 10);
        //             dos.writeInt(toWrite);
        //             startOfNumber = i + 1;
        //         }
                
        //     }
        //     line = br.readLine();

        // }

        // dos.close();

        CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
        CharBuffer cb2 = CharBuffer.allocate(4 * 1024);

        while (fr.read(cb1) != -1) {
            cb1.flip();
            int lastNumberStart = 0;
            for (int i = 0; i < cb1.length(); i++) {
                if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
                    int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);
                    dos.writeInt(numRead);
                    lastNumberStart = i + 1;
                }
            }

            cb2.clear();
            cb2.append(cb1, lastNumberStart, cb1.length());

            CharBuffer temp = cb2;
            cb2 = cb1;
            cb1 = temp;
        }

        fr.close();
        dos.close();
        

        long stop = System.currentTimeMillis();
        System.out.println(stop - start);
    }
}