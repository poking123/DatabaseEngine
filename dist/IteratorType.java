import java.util.Iterator;
import java.util.Queue;

public abstract class IteratorType implements Iterator<Queue<int[]>> {
	abstract String getType();
}