public class TableMetaData {
    // Keeps the number of columns
    // And the min, max, and unique for each column
    private int columns;
    private int[] min;
    private int[] max;
    private int[] unique;
    

    public TableMetaData(int columns) {
        this.columns = columns;
        min = new int[columns];
        max = new int[columns];
        unique = new int[columns];
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

    public int getColumns() {
        return this.columns;
    }

}