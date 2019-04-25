import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;

public class EquijoinWrite extends RAOperation {
	private Iterable<Queue<int[]>> source1;
	private Iterable<Queue<int[]>> source2;
    private String type;
    private int tempNumber;
	
	private EquijoinPredicate equijoinPredicate;

	public EquijoinWrite(RAOperation source1, RAOperation source2, EquijoinPredicate equijoinPredicate, int tempNumber) {
		this.source1 = source1;
		this.source2 = source2;

		this.equijoinPredicate = equijoinPredicate;
        this.type = "equijoin";
        
        this.tempNumber = tempNumber;
	}
	
	String getType() {
		return this.type;
	}
	
	@Override
	public Iterator<Queue<int[]>> iterator() {
		return new EquijoinWriteIterator(source1.iterator(), source2, equijoinPredicate.isTwoTableJoin(), equijoinPredicate.getTable1JoinCol(), equijoinPredicate.getTable2JoinCol(), this.tempNumber);
	}

	public class EquijoinWriteIterator implements Iterator<Queue<int[]>> {
		private Iterator<Queue<int[]>> source1Iterator;
		private Iterable<Queue<int[]>> source2;
		private boolean isTwoTableJoin;
		private int table1JoinCol;
        private int table2JoinCol;
        private int tempNumber;
        private int numOfRows;
        private int numOfCols;

        private HashMap<Integer, Queue<int[]>> bufferMap;
        
        private DataOutputStream dos;
		
		public EquijoinWriteIterator(Iterator<Queue<int[]>> source1Iterator, Iterable<Queue<int[]>> source2, boolean isTwoTableJoin, int table1JoinCol, int table2JoinCol, int tempNumber) {
			this.source1Iterator = source1Iterator;
			this.source2 = source2;
			this.isTwoTableJoin = isTwoTableJoin;
			
			this.table1JoinCol = table1JoinCol;
            this.table2JoinCol = table2JoinCol;
            
            this.tempNumber = tempNumber;
            this.numOfRows = 0;
            this.numOfCols = -1;

            bufferMap = new HashMap<>();

             try {
                dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(this.tempNumber + ".dat"))); 
             } catch (IOException e) {
                System.out.println("EquijoinWrite - FileNotFound");
             }
            
		}
		
		@Override
		public boolean hasNext() {
            if (source1Iterator.hasNext() == false) {
                try {
                    dos.close();
                } catch (IOException e) {
                    System.out.println("EquijoinWrite - IOException");
                }
                TableMetaData tmd = new TableMetaData("");
                tmd.setColumns(this.numOfCols);
                tmd.setRows(this.numOfRows);
                Catalog.addData(this.tempNumber + ".dat", tmd);
                return false;
            }
			return true;
		}

		@Override
		public Queue<int[]> next() {
			// next table1 Block
            Queue<int[]> input = source1Iterator.next();
			
			if (input.isEmpty()) { // this will also catch when we hasNext() is not true
				return null;
			} else {
				// BIG IF - Two table equijoin (tables are merged)
				if (this.isTwoTableJoin) {
					// Make HashMap for Hash BNLJ ////////////////////////////////////
					// table1JoinCol Value -> index in buffer
					bufferMap = new HashMap<>();
					while (!input.isEmpty()) {
						Queue<int[]> listOfIndices = new LinkedList<>();

						int[] table1Row = input.remove();
						int value = table1Row[this.table1JoinCol];
						
						if (bufferMap.containsKey(value)) {
							listOfIndices = bufferMap.get(value);
						}
						listOfIndices.add(table1Row);
						
						bufferMap.put(value, listOfIndices);
					}

					
					// just have to loop through table 2
					Iterator<Queue<int[]>> table2RowBlocks = source2.iterator();
					while (table2RowBlocks.hasNext()) {
						Queue<int[]> table2RowBlock = table2RowBlocks.next();
						while (!table2RowBlock.isEmpty()) {
							int[] table2Row = table2RowBlock.remove();
							int value = table2Row[this.table2JoinCol];
							if (bufferMap.containsKey(value)) {
								Queue<int[]> table1MatchingRows = new LinkedList<>(bufferMap.get(value));
								
								while (!table1MatchingRows.isEmpty()) {
                                    int[] combinedRow = combineRows(table1MatchingRows.remove(), table2Row);
                                    this.numOfCols = combinedRow.length;
									for (int i = 0; i < combinedRow.length; i++) {
                                        try {
                                            dos.writeInt(combinedRow[i]);
                                        } catch (IOException e) {
                                            System.out.println("EquijoinWrite - IOException");
                                        }
                                    }
                                    numOfRows++;
								}
							}
							// else, there are no matches

						}
					}

					return null;
				} else { // is one table join - we only scan table 1 because table 2's headers are already in table 1
					for (int[] table1Row : input) {
						if (equijoinPredicate.test(table1Row)) {
                            for (int i = 0; i < table1Row.length; i++) {
                                try {
                                    dos.writeInt(table1Row[i]);
                                } catch (IOException e) {
                                    System.out.println("EquijoinWrite - IOException");
                                }
                                
                            }
						}
					}
					return null;
				}
				
			}
			
		}
		
		public int[] combineRows(int[] row1, int[] row2) {
			int[] combinedRow = new int[row1.length + row2.length];

			System.arraycopy(row1, 0, combinedRow, 0, row1.length);
			System.arraycopy(row2, 0, combinedRow, row1.length, row2.length);
			
			return combinedRow;
		}
		
	}

	
	
	
	
	
	
	
}
