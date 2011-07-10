package MapReduce;
import org.apache.hadoop.io.ArrayWritable;

public class ContextInfoArray extends ArrayWritable {
	    public ContextInfoArray() {
	        super(Pair.class);
	    }
	    public ContextInfoArray(Pair[] values) {
	        super(Pair.class,values);
	    }
}
