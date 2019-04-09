
public class TableMetaData {
    // Keeps the number of columns
    // And the min, max, and unique for each column
    private int columns;
    private int rows;
    private int[] min;
    private int[] max;
    private int[] unique;
    private String columnNames;
    

    public TableMetaData(String columnNames) {
        this.columns = 0;
        this.rows = 0;
        min = new int[columns];
        max = new int[columns];
        unique = new int[columns];
        this.columnNames = columnNames;
    }
    
    public void setColumns(int columns) {
    	this.columns = columns;
    }
    
    public int getColumns() {
        return this.columns;
    }
    
    public void setRows(int rows) {
    	this.rows = rows;
    }
    
    public int getRows() {
        return this.rows;
    }

    public void setMin(int[] min) {
        this.min = min;
    }

    public int getMin(int column) {
        return this.min[column];
    }

    public void setMax(int[] max) {
        this.max = max;
    }

    public int getMax(int column) {
        return this.max[column];
    }

    public void setUnique(int[] unique) {
        this.unique = unique;
    }

    public int getUnique(int column) {
        return this.unique[column];
    }
    
    public int[] getUniqueColumns() {
    	return this.unique;
    }
    
    public String getColumnNames() {
        return this.columnNames;
    }


}