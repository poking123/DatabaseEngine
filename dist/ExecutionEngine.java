import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;

public class ExecutionEngine {
	
	public void executeQuery(Queue<Queue<RAOperation>> tablesQueue, Queue<Queue<Predicate>> predicatesQueue, Queue<Predicate> finalPredicateQueue, int[] columnsToSum) {
		
		Deque<RAOperation> finalDeque = new ArrayDeque<>();
		
		while (!predicatesQueue.isEmpty()) {
			

			Queue<RAOperation> tableQueue = tablesQueue.remove();
			Queue<Predicate> predicateQueue = predicatesQueue.remove();
			
			Deque<RAOperation> resultQueue = new ArrayDeque<>();
			
			while (!predicateQueue.isEmpty()) {
				// System.out.println("tableQueue: " + tableQueue);
				// System.out.println("predicateQueue: " + predicateQueue);
				// System.out.println("resultQueue: " + resultQueue);
				// System.out.println();
				Predicate currentPredicate = predicateQueue.remove();
				switch (currentPredicate.getType()) {
					case "filterPredicate": // take off from deque, filter, and put on result queue
						RAOperation operation = tableQueue.remove();
						FilterPredicate fp = (FilterPredicate) currentPredicate;
						Filter filter = new Filter(operation, fp);
						resultQueue.add(filter); // adds result to result queue
						break;
						
					case "equijoinPredicate": 
						// makes sure result queue size is 2
						while (resultQueue.size() < 2) {
							resultQueue.add(tableQueue.remove());
						}
						
						EquijoinPredicate ep = (EquijoinPredicate) currentPredicate;
						RAOperation table1 = resultQueue.remove();
						RAOperation table2 = resultQueue.remove();
						Equijoin equijoin = new Equijoin(table1, table2, ep);
						resultQueue.add(equijoin); // adds result to result queue
						break;

					case "disjointEquijoinPredicate":
						EquijoinPredicate ep2 = (EquijoinPredicate) currentPredicate;
						// gets two tables
						RAOperation disjointTable1 = tableQueue.remove();
						RAOperation disjointTable2 = tableQueue.remove();
						Equijoin disjointEquijoin = new Equijoin(disjointTable1, disjointTable2, ep2);
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
						MergeJoin mergeJoin = new MergeJoin(mgTable1, mgTable2, ep3);
						resultQueue.add(mergeJoin); // adds result to result queue
						break;
				}
			}
			finalDeque.add(resultQueue.remove());
		}
		
		while (!finalPredicateQueue.isEmpty()) {
			EquijoinPredicate currentPredicate = (EquijoinPredicate) finalPredicateQueue.remove();
			
			RAOperation table1 = finalDeque.remove();
			RAOperation table2 = finalDeque.remove();
			
			Equijoin disjointEquijoin = new Equijoin(table1, table2, currentPredicate);
			finalDeque.addFirst(disjointEquijoin);
		}
		
		// System.out.println("finalDeque: " + finalDeque);
		RAOperation finalOperation = finalDeque.pop(); // should only have 1 operation left in the queue
		// System.out.println("finalDeque: " + finalDeque);
		// System.out.println("final Operation is " + finalOperation);
		ProjectAndSum pas = new ProjectAndSum(finalOperation.iterator(), columnsToSum);
		while (pas.hasNext()) {
			pas.next();
		}
		System.out.println(pas.getSumString());
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