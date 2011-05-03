package MapReduce1;

/*
 * SortReducer.java
 *
 * Created on May 3, 2011, 6:25:01 PM
 */


import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author yarneo
 */
public class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("SortReducer");

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
        for(Text val : values) {
        	context.write(val,key);
        }
    }
}
