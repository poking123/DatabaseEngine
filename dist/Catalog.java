import java.util.HashMap;

public class Catalog {
    // Order: min, max, unique
    private HashMap<String, int[][]> metaDataMap;

    public Catalog() {
        metaDataMap = new HashMap<>();
    }

    public void addData(String fileName, int[][] data) {
        metaDataMap.put(fileName, data);
    }

    public int getValue(String filename, int column, int type) {
        if (metaDataMap.hasKey(fileName)) {
            return metaDataMap.get(fileName)[column][type];
        } else {
            System.out.println("Filename not found.");
            return -1;
        }
    }

    public getMin(String filename, int column) {
        return getValue(filename, column, 0);
    }

    public getMax(String filename, int column) {
        return getValue(filename, column, 1);
    }

    public getUnique(String filename, int column) {
        return getValue(filename, column, 2);
    }
}