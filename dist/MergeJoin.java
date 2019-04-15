import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;


public class MergeJoin extends RAOperation {
	private Iterable<Queue<int[]>> source1;
	private Iterable<Queue<int[]>> source2;
	private String type;
	private MergeJoinPredicate mergeJoinPredicate;
	
	public MergeJoin(RAOperation source1, RAOperation source2, MergeJoinPredicate mergeJoinPredicate) {
		this.source1 = source1;
		this.source2 = source2;
		this.mergeJoinPredicate = mergeJoinPredicate;
		this.type = "MergeJoin";
	}

	@Override
	public Iterator<Queue<int[]>> iterator() {
		try {
			return new MergeJoinIterator(source1.iterator(), source2.iterator(), mergeJoinPredicate.getTable1JoinCol(), mergeJoinPredicate.getTable2JoinCol());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	String getType() {
		return this.type;
	}
	
	public class MergeJoinIterator implements Iterator<Queue<int[]>> {

		private int table1JoinCol;
		private int table2JoinCol;

		private int table1Cols;
		private int table2Cols;
		
		private Queue<String> holder1;
		private Queue<String> holder2;
		
		private String table1FinalName;
		private String table2FinalName;
		
		private DataInputStream dis1;
		private DataInputStream dis2;
		
		private int dis1Value;
		private int dis2Value;
		
		private Queue<int[]> dis1Queue;
		private Queue<int[]> dis2Queue;
		
		private int[] dis1HolderRow;
		private int[] dis2HolderRow;

		private boolean table1Done;
		private boolean table2Done;

		private boolean noRows;
		private boolean noHasNext;
		
		public MergeJoinIterator(Iterator<Queue<int[]>> source1Iterator, Iterator<Queue<int[]>> source2Iterator, int table1JoinCol, int table2JoinCol) throws IOException {
			this.table1JoinCol = table1JoinCol;
			this.table2JoinCol = table2JoinCol;
			// System.out.println("table1JoinCol is " + table1JoinCol);
			// System.out.println("table2JoinCol is " + table2JoinCol);
			
			this.table1Cols = -1;
			this.table2Cols = -1;
			
			holder1 = new LinkedList<>();
			holder2 = new LinkedList<>();
			
			// External Sorting
			this.table1FinalName = writeSortedFileToDisk(source1Iterator, 1);
			this.table2FinalName = writeSortedFileToDisk(source2Iterator, 2);

			
			
			// System.out.println("table1FinalName is " + table1FinalName);
			// System.out.println("table2FinalName is " + table2FinalName);

			this.noHasNext = false;
			if (!(table1FinalName.equals("noRows") || table2FinalName.equals("noRows"))) {
				this.dis1 = Catalog.openStream(table1FinalName);
				this.dis2 = Catalog.openStream(table2FinalName);
				// for debugging - printing out all rows
				
				dis1Value = -1;
				dis2Value = -1;
				
				dis1Queue = new LinkedList<>();
				dis2Queue = new LinkedList<>();
				
				if (dis1.available() != 0 && dis2.available() != 0) {
					this.noRows = false;
					// adds the first row for each table and declares the join column value
					int[] dis1Row = new int[this.table1Cols];
					for (int i = 0; i < dis1Row.length; i++) {
						dis1Row[i] = dis1.readInt();
					}
					dis1Value = dis1Row[this.table1JoinCol];
					dis1Queue.add(dis1Row);
					if (dis1.available() == 0) {
						this.table1Done = true;
					} else {
						this.table1Done = false;
					}
					
					int[] dis2Row = new int[this.table2Cols];
					for (int i = 0; i < dis2Row.length; i++) {
						dis2Row[i] = dis2.readInt();
					}
					dis2Value = dis2Row[this.table2JoinCol];
					dis2Queue.add(dis2Row);
					if (dis2.available() == 0) {
						this.table2Done = true;
					} else {
						this.table2Done = false;
					}
						
				}
			} else {
				this.noRows = true;
			}
			
			
			
		}

		public void print(int[] arr) {
			for (int i : arr)
				System.out.print(i + " ");
			System.out.println();
		}

		
		public String toString() {
			return "merge join: cols " + this.table1JoinCol + " - " + this.table2JoinCol;
		}
		
		@Override
		public boolean hasNext() {
			return !this.noHasNext && (!table1Done || !table2Done);
		}

		@Override
		public Queue<int[]> next() {
			Queue<int[]> rowsToReturn = new LinkedList<int[]>();
			
			if (this.noRows) {
				this.noHasNext = true;
				return rowsToReturn;
			} 

			
			while (rowsToReturn.size() < DatabaseEngine.bufferSize) {
				// System.out.println("Rows to return size is " + rowsToReturn.size());
				// System.out.println("table1Done is " + table1Done);
				// System.out.println("table2Done is " + table2Done);

				try {
					if (!table1Done && !table2Done) {
						// System.out.println("dis1Value: " + dis1Value);
						// System.out.println("dis2Value: " + dis2Value);
						// System.out.println("table1Cols is " + this.table1Cols);
						// System.out.println("table2Cols is " + this.table2Cols);
						
						while (dis1Value != dis2Value && !table1Done && !table2Done) {
							// moves dis1Value up to dis2Value
							while (!table1Done && dis1Value < dis2Value) {
								this.dis1Queue.clear();
								int[] dis1Row = new int[this.table1Cols];
								for (int i = 0; i < this.table1Cols; i++) {
									dis1Row[i] = dis1.readInt();
								}
								if (dis1.available() == 0) table1Done = true;
								// new dis1Value
								dis1Value = dis1Row[this.table1JoinCol];
								dis1Queue.add(dis1Row);
							}
							// moves dis2Value up to dis1Value
							while (!table2Done && dis2Value < dis1Value) {
								this.dis2Queue.clear();
								int[] dis2Row = new int[this.table2Cols];
								for (int i = 0; i < this.table2Cols; i++) {
									dis2Row[i] = dis2.readInt();
								}
								if (dis2.available() == 0) table2Done = true;
								// new dis2Value
								dis2Value = dis2Row[this.table2JoinCol];
								dis2Queue.add(dis2Row);
							}
						}
						
						if (dis1Value != dis2Value) {
							this.noRows = true;
							break;
						}
						
						// Boths values are equal
						// add same join col rows to dis1Queue
						int[] dis1Row = new int[this.table1Cols];
						if (!table1Done) {
							for (int i = 0; i < this.table1Cols; i++) {
								dis1Row[i] = dis1.readInt();
							}
							if (dis1.available() == 0) {
								table1Done = true;
							}
						} else {
							dis1Row = dis1Queue.remove();
						}
						
						
						while (dis1Row[this.table1JoinCol] == dis1Value) {
							
							// System.out.println("Added table 1 row");
							// print(dis1Row);
							dis1Queue.add(Arrays.copyOf(dis1Row, dis1Row.length));
							// System.out.println("dis1Queue size is " + dis1Queue.size());
							if (!table1Done) {
								for (int i = 0; i < this.table1Cols; i++) {
									dis1Row[i] = dis1.readInt();
								}
								if (dis1.available() == 0) {
									table1Done = true;
								}
							} else {
								break;
							}
						}
						if (dis1Row[this.table1JoinCol] == dis1Value) {
							dis1HolderRow = null;
						} else {
							dis1HolderRow = dis1Row;
						}
						
						
						// add same join col rows to dis2Queue
						int[] dis2Row = new int[this.table2Cols];
						if (!table2Done) {
							for (int i = 0; i < this.table2Cols; i++) {
								dis2Row[i] = dis2.readInt();
							}
							if (dis2.available() == 0) {
								table2Done = true;
							}
						} else {
							dis2Row = dis2Queue.remove();
						}
						
						while (dis2Row[this.table2JoinCol] == dis2Value) {
							dis2Queue.add(Arrays.copyOf(dis2Row, dis2Row.length));
							if (!table2Done) {
								for (int i = 0; i < this.table2Cols; i++) {
									dis2Row[i] = dis2.readInt();
								}
								if (dis2.available() == 0) {
									table2Done = true;
								}
							} else {
								break;
							}
						}
						if (dis2Row[this.table2JoinCol] == dis2Value) {
							dis2HolderRow = null;
						} else {
							dis2HolderRow = dis2Row;
						}


						// System.out.println("Make Combinations");
						// add all combinations of matched rows
						makeCombinations(rowsToReturn);
						
						// add holders to empty queues
						dis2Queue.clear();
						
						if (dis1HolderRow != null) {
							dis1Value = dis1HolderRow[this.table1JoinCol];
							dis1Queue.add(Arrays.copyOf(dis1HolderRow, dis1HolderRow.length));
						}
							
						if (dis2HolderRow != null) {
							dis2Value = dis2HolderRow[this.table2JoinCol];
							dis2Queue.add(Arrays.copyOf(dis2HolderRow, dis2HolderRow.length));
						}
							
						
					} else {
						if(table1Done && table2Done) {
							if (dis1Queue.size() == 1 && dis2Queue.size() == 1)
								rowsToReturn.add(combineRows(dis1Queue.remove(), dis2Queue.remove()));
							break;
						}

						if (table2Done) {
							// System.out.println("Done reading Table2");
							while (!table1Done && dis1Value < dis2Value) {
								this.dis1Queue.clear();
								int[] dis1Row = new int[this.table1Cols];
								for (int i = 0; i < this.table1Cols; i++) {
									dis1Row[i] = dis1.readInt();
								}
								if (dis1.available() == 0) table1Done = true;
								// new dis1Value
								dis1Value = dis1Row[this.table1JoinCol];
								dis1Queue.add(dis1Row);
							}

							if (dis1Value == dis2Value) {
								int[] dis1Row = dis1Queue.remove();
								while (dis1Row[this.table1JoinCol] == dis1Value) {
									dis1Queue.add(Arrays.copyOf(dis1Row, dis1Row.length));
									if (!table1Done) {
										for (int i = 0; i < this.table1Cols; i++) {
											dis1Row[i] = dis1.readInt();
										}
										if (dis1.available() == 0) {
											table1Done = true;
										}
									} else {
										break;
									}
								}
							
								makeCombinations(rowsToReturn);
							}
							table1Done = true;
						} else if (table1Done) {
							// System.out.println("got in table1Done after file already read");
							while (!table2Done && dis2Value < dis1Value) {
								this.dis2Queue.clear();
								int[] dis2Row = new int[this.table2Cols];
								for (int i = 0; i < this.table2Cols; i++) {
									dis2Row[i] = dis2.readInt();
								}
								if (dis2.available() == 0) table2Done = true;
								// new dis2Value
								dis2Value = dis2Row[this.table2JoinCol];
								dis2Queue.add(dis2Row);
							}


							if (dis1Value == dis2Value) {
								int[] dis2Row = dis2Queue.remove();
								while (dis2Row[this.table2JoinCol] == dis2Value) {
									dis2Queue.add(Arrays.copyOf(dis2Row, dis2Row.length));
									if (!table2Done) {
										for (int i = 0; i < this.table2Cols; i++) {
											dis2Row[i] = dis2.readInt();
										}
										if (dis2.available() == 0) {
											table2Done = true;
										}
									} else {
										break;
									}
								}
							
								makeCombinations(rowsToReturn);
							}
							table2Done = true;
						}

						break;
					}
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
					break;
				}
			}
				
			return rowsToReturn;
		}

		public void makeCombinations(Queue<int[]> rowsToReturn) {
			int[] dis1Row;
			int[] dis2Row;
			while (!dis1Queue.isEmpty()) {
				// System.out.println("dis1Queue size is " + dis1Queue.size());
				dis1Row = dis1Queue.remove();
				// System.out.println("dis1Row:");
				// for (int i : dis1Row)
				// 	System.out.print(i + " ");
				// System.out.println();
				// System.out.println("dis2Rows:");
				Iterator<int[]> dis2QueueItr = dis2Queue.iterator();
				while (dis2QueueItr.hasNext()) {
					dis2Row = dis2QueueItr.next();
					// for (int i : dis2Row)
					// 	System.out.print(i + " ");
					// System.out.println();
					// for (int i : combineRows(dis1Row, dis2Row))
					// 	System.out.print(i + " ");
					// System.out.println();
					rowsToReturn.add(combineRows(dis1Row, dis2Row));
				}
			}
		}
		
		
		public String writeSortedFileToDisk(Iterator<Queue<int[]>> iterator, int table) throws IOException {
			// System.out.println("table " + table);

			// Writes both tables to disk, sorted on each join column
			while (iterator.hasNext()) {
				Queue<int[]> tableRows = iterator.next();
				if (tableRows.size() == 0) continue;

				int tempNumber = DatabaseEngine.tempNumber;
				DatabaseEngine.tempNumber++;
				DataOutputStream tempDOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
				holder1.add(tempNumber + ".dat");

				int tableJoinCol = -1;
				if (table == 1) {
					this.table1Cols = tableRows.peek().length;
					tableJoinCol = this.table1JoinCol;
				} else if (table == 2) {
					this.table2Cols = tableRows.peek().length;
					tableJoinCol = this.table2JoinCol;
					// System.out.println("In table 2");
				}
				
				
				TreeMap<Integer, Queue<int[]>> columnValueToRowsMap = new TreeMap<>();
				while (!tableRows.isEmpty()) { // writes all rows to map
					// System.out.println("writes all rows to map");
					Queue<int[]> valueRows = new LinkedList<>();
					int[] tableRow = tableRows.remove();
					int tableJoinColValue = tableRow[tableJoinCol];
					if (columnValueToRowsMap.containsKey(tableJoinColValue)) {
						valueRows = columnValueToRowsMap.get(tableJoinColValue);
					}
					valueRows.add(tableRow);
					columnValueToRowsMap.put(tableJoinColValue, valueRows);
				}
				
				// writes the sorted keyset
				while (!columnValueToRowsMap.isEmpty()) {
					// System.out.println("writes the sorted keyset");
					int key = columnValueToRowsMap.firstKey();
					Queue<int[]> tableSortedRows = columnValueToRowsMap.get(key);
					while (!tableSortedRows.isEmpty()) {
						int[] tableSortedRow = tableSortedRows.remove();
						for (int i : tableSortedRow) {
							tempDOS.writeInt(i);
							// System.out.print(i + " ");
						}
						// System.out.println();
					}
					columnValueToRowsMap.remove(key);
				}
				tempDOS.close();
			}

			if (holder1.size() == 0) return "noRows";
			
			// External Sort on the sorted blocks
			while (holder1.size() != 1) {
				while (!holder1.isEmpty()) {
					if (holder1.size() == 1) {
						holder2.add(holder1.remove());
					} else {
						String table1 = holder1.remove();
						String table2 = holder1.remove();
						
						mergeFiles(table1, table2, table);
					}
				}
				
				holder1 = holder2;
				holder2 = new LinkedList<>();
			}
			
			return holder1.remove();
		}
		
		public void mergeFiles(String table1, String table2, int table) throws IOException {
			int tableCols = (table == 1) ? this.table1Cols : this.table2Cols;
			int tableJoinCol = (table == 1) ? this.table1JoinCol : this.table2JoinCol;
			// System.out.println("tableCols is " + tableCols);
			// System.out.println("tableJoinCol is " + tableJoinCol);
			
			int tempNumber = DatabaseEngine.tempNumber;
			DatabaseEngine.tempNumber++;
			DataOutputStream tempDOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
			
			holder2.add(tempNumber + ".dat");
			
			DataInputStream dis1 = Catalog.openStream(table1);
			DataInputStream dis2 = Catalog.openStream(table2);
			
			// fills the rows for both tables
			int[] table1TempRow = new int[tableCols];
			int[] table2TempRow = new int[tableCols];
			
			boolean writeTable1 = true;
			boolean writeTable2 = true;

			boolean table1Done = false;
			boolean table2Done = false;
			while (!table1Done && !table2Done) {
			//while (dis1.available() != 0 && dis2.available() != 0) {
				// System.out.println("In merge files loop");
				// System.out.println(dis1.available());
				// System.out.println(dis2.available());
				// System.out.println();

				if (writeTable1) {
					for (int i = 0; i < tableCols; i++) {
						int value = dis1.readInt();
						table1TempRow[i] = value;
					}
					writeTable1 = false;
				}
				
				if (writeTable2) {

					for (int i = 0; i < tableCols; i++) {
						int value = dis2.readInt();
						table2TempRow[i] = value;
					}
					writeTable2 = false;
				}
				
				
				int table1JoinColValue = table1TempRow[tableJoinCol];
				int table2JoinColValue = table2TempRow[tableJoinCol];

				// System.out.println("table1JoinColValue is " + table1JoinColValue);
				// System.out.println("table2JoinColValue is " + table2JoinColValue);
				
				// write the smaller row
				if (table1JoinColValue < table2JoinColValue) {
					// System.out.println("Writing table 1");
					
					for (int i : table1TempRow) {
						// System.out.print(i + " ");
						tempDOS.writeInt(i);
					}
					// System.out.println();
					writeTable1 = true;
					if (dis1.available() == 0) table1Done = true;
					
				} else {
					// System.out.println("Writing table 2");
					for (int i : table2TempRow) {
						tempDOS.writeInt(i);
						// System.out.print(i + " ");
					}
					// System.out.println();
					writeTable2 = true;
					if (dis2.available() == 0) table2Done = true;
				}
			}
			
			if (table1Done) {
				// System.out.println("Writing rest of dis2");
				for (int i : table2TempRow) {
					// System.out.println(i + " ");
					tempDOS.writeInt(i);
				}
				// System.out.println();

				while (dis2.available() != 0) {
					for (int i = 0; i < tableCols; i++) {
						int value = dis2.readInt();
						// System.out.print(value + " ");
						tempDOS.writeInt(value);
					}
					// System.out.println();
				}
			} else if (table2Done) {
				// System.out.println("Writing rest of dis1");
				for (int i : table1TempRow) {
					// System.out.print(i + " ");
					tempDOS.writeInt(i);
				}
				// System.out.println();

				while (dis1.available() != 0) {
					for (int i = 0; i < tableCols; i++) {
						int value = dis1.readInt();
						// System.out.print(value + " ");
						tempDOS.writeInt(value);
					}
					// System.out.println();
				}
			}
			
			tempDOS.close();
		}

		public int[] combineRows(int[] row1, int[] row2) {
			int[] combinedRow = new int[row1.length + row2.length];
			int i = 0;
			int totalIndex = 0;
			
			while (i < row1.length) {
				combinedRow[totalIndex] = row1[totalIndex];
				i++;
				totalIndex++;
			}
			
			i = 0;
			
			while (i < row2.length) {
				combinedRow[totalIndex] = row2[i];
				i++;
				totalIndex++;
			}
			
			return combinedRow;
		}
		
	}

}