
/*
 * RowReducer.java
 *
 * Created on May 8, 2011, 7:20:40 PM
 */

package MapReduce1;


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
public class RowReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("MapReduce1.RowReducer");

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                        throws IOException, InterruptedException {
    	long finalval = 0;
        for(LongWritable val : values) {
        	finalval+=val.get();
        }
        context.write(key, new LongWritable(finalval));
    }
}
