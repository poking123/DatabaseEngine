import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;

public class ExecutionEngine {

	public void executeQuery(Queue<Queue<RAOperation>> tablesQueue, Queue<Queue<Predicate>> predicatesQueue, Queue<Predicate> finalPredicateQueue, int[] columnsToSum, Queue<Queue<Boolean>> switchesQueue) throws IOException {
		
		Deque<RAOperation> finalDeque = new ArrayDeque<>();
		
		while (!predicatesQueue.isEmpty()) {
			

			Queue<RAOperation> tableQueue = tablesQueue.remove();
			Queue<Predicate> predicateQueue = predicatesQueue.remove();
			Queue<Boolean> switchQueue = switchesQueue.remove();

			// System.err.println(tableQueue);
			// System.err.println(predicateQueue);
			// System.err.println(switchQueue);
			
			Deque<RAOperation> resultQueue = new ArrayDeque<>();
			
			while (!predicateQueue.isEmpty()) {
				// System.out.println("tableQueue: " + tableQueue);
				// System.out.println("predicateQueue: " + predicateQueue);
				// System.out.println("resultQueue: " + resultQueue);
				// System.out.println();
				Predicate currentPredicate = predicateQueue.remove();
				switch (currentPredicate.getType()) {
					case "placePredicate": // just put a table from tableQueue onto resultQueue
						resultQueue.add(tableQueue.remove());
						break;

					case "filterPredicate": // take off from deque, filter, and put on result queue
						FilterPredicate fp = (FilterPredicate) currentPredicate;
						FilterProjectScan filterProjectScan = new FilterProjectScan(fp);
						resultQueue.add(filterProjectScan); // adds result to result queue
						break;
						
					case "equijoinPredicate": 
						// makes sure result queue size is 2
						while (resultQueue.size() < 2) {
							resultQueue.add(tableQueue.remove());
						}
						
						EquijoinPredicate ep = (EquijoinPredicate) currentPredicate;

						RAOperation table1 = resultQueue.remove();
						RAOperation table2 = resultQueue.remove();

						Equijoin equijoin = new Equijoin(table1, table2, null, ep); // dummy declaration
						if (predicateQueue.isEmpty()) { // last join
							// Equijoin equijoin = new Equijoin(table1, table2, columnsToSum, ep); // dummy declaration
							if (switchQueue.remove()) {
								int tempNumber = DatabaseEngine.tempNumber;
								DatabaseEngine.tempNumber++;
								// DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
								FileChannel fc = new RandomAccessFile(tempNumber + ".dat", "rw").getChannel();
								IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE).asIntBuffer();

								int numOfCols = -1;
								int numOfRows = 0;

								Iterator<Queue<int[]>> table1Itr = table1.iterator();
								while (table1Itr.hasNext()) {
									Queue<int[]> table1Rows = table1Itr.next();
									while (!table1Rows.isEmpty()) {
										int[] table1Row = table1Rows.remove();
										numOfCols = table1Row.length;
										for (int i = 0; i < table1Row.length; i++) {
											// dos.writeInt(table1Row[i]);
											ib.put(table1Row[i]);
										}
										numOfRows++;
									}
								}
								// dos.close();
								fc.close();

								TableMetaData tmd = new TableMetaData("");
								tmd.setColumns(numOfCols);
								tmd.setRows(numOfRows);
								Catalog.addData(tempNumber + ".dat", tmd);

								equijoin = new Equijoin(table2, new Scan(tempNumber + ".dat", null), columnsToSum,  ep);
							} else {
								equijoin = new Equijoin(table1, table2, columnsToSum,  ep);
							}
						} else {
							if (switchQueue.remove()) {
								int tempNumber = DatabaseEngine.tempNumber;
								DatabaseEngine.tempNumber++;
								// DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempNumber + ".dat")));
								FileChannel fc = new RandomAccessFile(tempNumber + ".dat", "rw").getChannel();
								IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE).asIntBuffer();
								
								int numOfCols = -1;
								int numOfRows = 0;

								// Go through table 1 and write the result to disk
								Iterator<Queue<int[]>> table1Itr = table1.iterator();
								while (table1Itr.hasNext()) {
									Queue<int[]> table1Rows = table1Itr.next();
									while (!table1Rows.isEmpty()) {
										int[] table1Row = table1Rows.remove();
										numOfCols = table1Row.length;
										for (int i = 0; i < table1Row.length; i++) {
											// dos.writeInt(table1Row[i]);
											ib.put(table1Row[i]);
										}
										numOfRows++;
									}
								}
								fc.close();
								// dos.close();

								TableMetaData tmd = new TableMetaData("");
								tmd.setColumns(numOfCols);
								tmd.setRows(numOfRows);
								Catalog.addData(tempNumber + ".dat", tmd);

								equijoin = new Equijoin(table2, new Scan(tempNumber + ".dat", null), null, ep);
							} else {
								equijoin = new Equijoin(table1, table2, null, ep);
							}
						}
						

						// Equijoin equijoin = new Equijoin(table1, table2, ep);
						//Equijoin equijoin = new Equijoin(table2, table1, ep); // making hash on smaller table?
						resultQueue.add(equijoin); // adds result to result queue
						break;

					case "disjointEquijoinPredicate":
						EquijoinPredicate ep2 = (EquijoinPredicate) currentPredicate;
						// gets two tables
						RAOperation disjointTable1 = tableQueue.remove();
						RAOperation disjointTable2 = tableQueue.remove();
						Equijoin disjointEquijoin = new Equijoin(disjointTable1, disjointTable2, null, ep2);

						resultQueue.add(disjointEquijoin);
						break;

					case "mergeJoinPredicate": 
						// makes sure result queue size is 2
						while (resultQueue.size() < 2) {
							resultQueue.add(tableQueue.remove());
						}
						
						MergeJoinPredicate ep3 = (MergeJoinPredicate) currentPredicate;
						RAOperation mgTable1 = resultQueue.remove();
						RAOperation mgTable2 = resultQueue.remove();

						MergeJoin mergeJoin = new MergeJoin(mgTable1, mgTable2, ep3, null);
						if (predicateQueue.isEmpty()) {
							if (switchQueue.remove()) { // not needed because it's merge join?
								mergeJoin = new MergeJoin(mgTable2, mgTable1, ep3, columnsToSum);
							} else {
								mergeJoin = new MergeJoin(mgTable1, mgTable2, ep3, columnsToSum);
							}
						} else {
							if (switchQueue.remove()) { // not needed because it's merge join?
								mergeJoin = new MergeJoin(mgTable2, mgTable1, ep3, null);
							} else {
								mergeJoin = new MergeJoin(mgTable1, mgTable2, ep3, null);
							}
						}

						// MergeJoin mergeJoin = new MergeJoin(mgTable1, mgTable2, ep3);
						resultQueue.add(mergeJoin); // adds result to result queue
						break;

					case "equijoinWritePredicate":
						// perform an equijoin and put a scan of the file name in the resultQueue
						// makes sure result queue size is 2
						while (resultQueue.size() < 2) {
							resultQueue.add(tableQueue.remove());
						}
						
						EquijoinPredicate ep4 = (EquijoinPredicate) currentPredicate;

						RAOperation EJWritetable1 = resultQueue.remove();
						RAOperation EJWritetable2 = resultQueue.remove();

						int tempNumber = DatabaseEngine.tempNumber;
						DatabaseEngine.tempNumber++;

						EquijoinWrite equijoinWrite = new EquijoinWrite(EJWritetable1, EJWritetable2, ep4, tempNumber, null); // dummy declaration
						if (predicateQueue.isEmpty()) {
							if (switchQueue.remove()) {
								equijoinWrite = new EquijoinWrite(EJWritetable2, EJWritetable1, ep4, tempNumber, columnsToSum);
							} else {
								equijoinWrite = new EquijoinWrite(EJWritetable1, EJWritetable2, ep4, tempNumber, columnsToSum);
							}
							resultQueue.add(equijoinWrite);
						} else {
							if (switchQueue.remove()) {
								equijoinWrite = new EquijoinWrite(EJWritetable2, EJWritetable1, ep4, tempNumber, null);
							} else {
								equijoinWrite = new EquijoinWrite(EJWritetable1, EJWritetable2, ep4, tempNumber, null);
							}
	
							Iterator<Queue<int[]>> equijoinWriteItr = equijoinWrite.iterator();
							while (equijoinWriteItr.hasNext()) { // write the new file to disk
								equijoinWriteItr.next();
							}
	
							try {
								resultQueue.add(new Scan(tempNumber + ".dat", null)); // adds result to result queue
							} catch (FileNotFoundException e) {
								System.out.println("ExecutionEngine - equjoinWrite - FileNotFoundException");
							}
						}

						
						
						break;
				}
			}
			finalDeque.add(resultQueue.remove());
		}
		
		while (!finalPredicateQueue.isEmpty()) {
			EquijoinPredicate currentPredicate = (EquijoinPredicate) finalPredicateQueue.remove();
			
			RAOperation table1 = finalDeque.remove();
			RAOperation table2 = finalDeque.remove();
			
			Equijoin disjointEquijoin = new Equijoin(table1, table2, null, currentPredicate);
			finalDeque.addFirst(disjointEquijoin);
		}
		
		// System.out.println("finalDeque: " + finalDeque);
		RAOperation finalOperation = finalDeque.pop(); // should only have 1 operation left in the queue
		// System.out.println("finalDeque: " + finalDeque);
		// System.out.println("final Operation is " + finalOperation);
		// ProjectAndSum pas = new ProjectAndSum(finalOperation.iterator(), columnsToSum);
		// while (pas.hasNext()) {
		// 	pas.next();
		// }
		// System.out.println(pas.getSumString());
		Iterator<Queue<int[]>> finalOperationItr = finalOperation.iterator();

		while (finalOperationItr.hasNext()) {
			finalOperationItr.next();
		}
	}
	
	public int findIndex(String[] columnNames, String columnName) {
		int i = 0;
		while (i < columnNames.length) {
			if (columnNames[i].equals(columnName)) return i;
			i++;
		}
		return -1;
	}
}