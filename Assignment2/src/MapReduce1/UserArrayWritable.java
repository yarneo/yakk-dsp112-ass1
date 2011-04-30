package MapReduce1;
import org.apache.hadoop.io.ArrayWritable;

public class UserArrayWritable extends ArrayWritable {
	    public UserArrayWritable() {
	        super(ContextsUserWritable.class);
	    }
	    public UserArrayWritable(ContextsUserWritable[] values) {
	        super(ContextsUserWritable.class,values);
	    }
}
