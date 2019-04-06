import java.util.HashMap;

public class Catalog {
    // Order: min, max, unique
   // private HashMap<String, int[][]> metaDataMap;
    private HashMap<String, TableMetaData> metaDataMap;

    // A string (file name) will map to the data

    public Catalog() {
        metaDataMap = new HashMap<>();
    }

    public void addData(String fileName, TableMetaData metadata) {
        metaDataMap.put(fileName, metadata);
    }

    public int getMin(String tableName, int column) {
        return metaDataMap.get(tableName).getMin(column);
    }

    public int getMax(String tableName, int column) {
        return metaDataMap.get(tableName).getMax(column);
    }

    public int getUnique(String tableName, int column) {
        return metaDataMap.get(tableName).getUnique(column);
    }

    public int getColumns(String tableName) {
        return metaDataMap.get(tableName).getColumns();
    }
    
    public String getColumnNames(String tableName) {
    	return metaDataMap.get(tableName).getColumnNames();
    }

}