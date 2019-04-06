public class TableMetaData {
    // Keeps the number of columns
    // And the min, max, and unique for each column
    private int columns;
    private int rows;
    private int[] min;
    private int[] max;
    private int[] unique;
    private String columnNames;
    

    public TableMetaData(int columns, int rows, String columnNames) {
        this.columns = columns;
        this.rows = rows;
        min = new int[columns];
        max = new int[columns];
        unique = new int[columns];
        this.columnNames = columnNames;
    }
    
    public int getColumns() {
        return this.columns;
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

    public void setColumns(int columns) {
        this.columns = columns;
    }
    
    public String getColumnNames() {
        return this.columnNames;
    }


}