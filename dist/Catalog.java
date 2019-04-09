
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;

public class Catalog {
	
    private static HashMap<String, TableMetaData> metaDataMap = new HashMap<>();
    
    public static DataInputStream openStream(String tableName) throws FileNotFoundException {
    	return new DataInputStream(new BufferedInputStream(new FileInputStream(tableName)));
    }
    
    public static boolean containsTable(String tableName) {
    	return metaDataMap.containsKey(tableName);
    }

    public static void addData(String fileName, TableMetaData metadata) {
        metaDataMap.put(fileName, metadata);
    }
    
    public static void removeData(String fileName) {
    	metaDataMap.remove(fileName);
    }

    public static int getMin(String tableName, int column) {
        return metaDataMap.get(tableName).getMin(column);
    }

    public static int getMax(String tableName, int column) {
        return metaDataMap.get(tableName).getMax(column);
    }

    public static int getUnique(String tableName, int column) {
        return metaDataMap.get(tableName).getUnique(column);
    }
    
    public static int[] getUniqueColumns(String tableName) {
    	return metaDataMap.get(tableName).getUniqueColumns();
    }

    public static int getColumns(String tableName) {
        return metaDataMap.get(tableName).getColumns();
    }
    
    public static String getHeader(String tableName) {
    	return metaDataMap.get(tableName).getColumnNames();
    }
    
    public static int getRows(String tableName) {
    	return metaDataMap.get(tableName).getRows();
    }

}