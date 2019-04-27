import java.io.BufferedOutputStream;
import java.io.DataInputStream;
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

public class GraceHashJoin extends RAOperation {
    private Iterable<Queue<int[]>> source1;
    private Iterable<Queue<int[]>> source2;
    private String type;

    private EquijoinPredicate equijoinPredicate;

    public GraceHashJoin(RAOperation source1, RAOperation source2, EquijoinPredicate equijoinPredicate) {
        this.source1 = source1;
        this.source2 = source2;

        this.equijoinPredicate = equijoinPredicate;
        this.type = "graceHashJoin";
    }

    String getType() {
        return this.type;
    }

    @Override
    public Iterator<Queue<int[]>> iterator() {
        // NEED TO CHANGE PREDICATE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        return new GraceHashIterator(source1.iterator(), source2.iterator(), equijoinPredicate.getTable1JoinCol(),
                equijoinPredicate.getTable2JoinCol());
    }

    public class GraceHashIterator implements Iterator<Queue<int[]>> {
        private Iterator<Queue<int[]>> source1Iterator;
        private Iterator<Queue<int[]>> source2Iterator;
        private int table1JoinCol;
        private int table2JoinCol;

        private HashMap<Integer, Queue<int[]>> bufferMap;

        private Queue<String> table1Partitions;
        private Queue<String> table2Partitions;

        private Queue<Integer> table1PartitionSizes;
        private Queue<Integer> table2PartitionSizes;

        private int table1Cols;
        private int table2Cols;

        private int numOfPartitions;

        public GraceHashIterator(Iterator<Queue<int[]>> source1Iterator, Iterator<Queue<int[]>> source2Iterator, int table1JoinCol, int table2JoinCol) {
            this.source1Iterator = source1Iterator;
            this.source2Iterator = source2Iterator;

            this.table1Partitions = new LinkedList<>();
            this.table2Partitions = new LinkedList<>();

            this.table1JoinCol = table1JoinCol;
            this.table2JoinCol = table2JoinCol;

            this.numOfPartitions = 10;

            table1PartitionSizes = new LinkedList<>();
            table2PartitionSizes = new LinkedList<>();

            // makes the partitions
            try {
                makePartitions(source1Iterator, table1Partitions, table1JoinCol, table1PartitionSizes, 1);
                makePartitions(source2Iterator, table2Partitions, table2JoinCol, table2PartitionSizes, 2);
            } catch (IOException e) {
                System.out.println("Grace Hash Join - IOException");
            }
        }

        public void makePartitions(Iterator<Queue<int[]>> sourceIterator, Queue<String> tablePartitions, int joinCol, Queue<Integer> tablePartionSizes, int table) throws IOException {
            DataOutputStream[] outputStreams = new DataOutputStream[this.numOfPartitions];
            int[] partitionSizes = new int[this.numOfPartitions];
            for (int i = 0; i < numOfPartitions; i++) {
                int tempNumber = DatabaseEngine.tempNumber;
                DatabaseEngine.tempNumber++;

                outputStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
                tablePartitions.add(tempNumber + ".dat");
            }

            while (sourceIterator.hasNext()) {
                Queue<int[]> rows = sourceIterator.next();
                while (!rows.isEmpty()) {
                    int[] row = rows.remove();
                    if (table == 1) {
                        this.table1Cols = row.length;
                    } else if (table == 2) {
                        this.table2Cols = row.length;
                    }
                    int joinValue = row[joinCol];
                    int partition = joinValue % numOfPartitions;
                    partition = (partition < 0) ? (partition + numOfPartitions) : partition; // makes sure partition is positive
                    for (int i = 0; i < numOfPartitions; i++) {
                        outputStreams[partition].writeInt(row[i]);
                    }
                    partitionSizes[partition]++;
                }
            }
            
            // closes all streams
            for (int i = 0; i < numOfPartitions; i++) {
                outputStreams[i].close();
                tablePartionSizes.add(partitionSizes[i]);
            }
        }
		
		@Override
		public boolean hasNext() {

			return !table1Partitions.isEmpty();
		}

		@Override
		public Queue<int[]> next() {
				
            Queue<int[]> rowsToReturn = new LinkedList<>();
            
            while (rowsToReturn.size() < DatabaseEngine.bufferSize) {
                String table1 = "";
                String table2 = "";

                int table1Size = this.table1PartitionSizes.remove();
                int table2Size = this.table2PartitionSizes.remove();

                if (table1Size < table2Size) {
                    table1 = this.table1Partitions.remove();
                    table2 = this.table2Partitions.remove();
                } else {
                    table1 = this.table2Partitions.remove();
                    table2 = this.table1Partitions.remove();
                }

                HashMap<Integer, Queue<int[]>> bufferMap = new HashMap<>();
                // DataInputStream dis1 = Catalog.openStream(table1);
                // DataInputStream dis2 = Catalog.openStream(table2);
                // while (dis1.available() != 0) {
                //     if (table1Size < table2Size) {
                //         int[] table1Row = new int[this.table1Cols];
                //         for (int i = 0; i < this.table1Cols; i++) {
                //             table1Row[i] = dis1.readInt();
                //         }
                //         int value = table1Row[this.table1JoinCol];
                //         Queue<int[]> rowsToPut = new LinkedList<>();
                //         if (bufferMap.containsKey(value)) {
                //             rowsToPut = new LinkedList<>(bufferMap.get(value));
                //         }
                //         bufferMap.put(value, rowsToPut);
                //     } else {
                //         int[] table1Row = new int[this.table2Cols];
                //             for (int i = 0; i < this.table2Cols; i++) {
                //                 table1Row[i] = dis2.readInt();
                //             }
                //             int value = table1Row[this.table1JoinCol];
                //             Queue<int[]> rowsToPut = new LinkedList<>();
                //             if (bufferMap.containsKey(value)) {
                //                 rowsToPut = new LinkedList<>(bufferMap.get(value));
                //             }
                //             bufferMap.put(value, rowsToPut);
                //     }
                // } 


                
            }

            // Make HashMap for Hash BNLJ ////////////////////////////////////
            // table1JoinCol Value -> index in buffer
            // System.out.println("Made Buffer Map");
            // bufferMap = new HashMap<>();
            // while (!input.isEmpty()) {
            //     Queue<int[]> listOfIndices = new LinkedList<>();

            //     int[] table1Row = input.remove();
            //     int value = table1Row[this.table1JoinCol];
                
            //     if (bufferMap.containsKey(value)) {
            //         listOfIndices = bufferMap.get(value);
            //     }
            //     listOfIndices.add(table1Row);
                
            //     bufferMap.put(value, listOfIndices);
            // }

            
            // just have to loop through table 2
            // table2RowBlocks = source2.iterator();
            // while (table2RowBlocks.hasNext()) {
            //     table2RowBlock = table2RowBlocks.next();
            //     while (!table2RowBlock.isEmpty()) {
            //         this.table2Row = table2RowBlock.remove();
            //         int value = table2Row[this.table2JoinCol];
            //         if (bufferMap.containsKey(value)) {
            //             this.table1MatchingRows = new LinkedList<>(bufferMap.get(value));
                        
            //             while (!this.table1MatchingRows.isEmpty()) {
            //                 // int[] table1MatchingRow = table1MatchingRows.remove();
            //                 rowsToReturn.add(combineRows(table1MatchingRows.remove(), table2Row));

            //                 if (rowsToReturn.size() > DatabaseEngine.equijoinBufferSize) {										
            //                     return rowsToReturn;
            //                 }
            //             }
            //         }
            //         // else, there are no matches

            //     }
            // }
            // System.out.println("went through all of table2 and returned");


			
			return null;
		}
		
		public int[] combineRows(int[] row1, int[] row2) {
			int[] combinedRow = new int[row1.length + row2.length];

			System.arraycopy(row1, 0, combinedRow, 0, row1.length);
			System.arraycopy(row2, 0, combinedRow, row1.length, row2.length);
			
			return combinedRow;
		}
		
	}

	
	
	
	
	
	
	
}
