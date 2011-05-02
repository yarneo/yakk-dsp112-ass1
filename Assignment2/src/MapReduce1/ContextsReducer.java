
/*
 * ContextsReducer.java
 *
 * Created on Apr 30, 2011, 3:52:13 PM
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
public class ContextsReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("MapReduce2.ContextsReducer");

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                        throws IOException, InterruptedException {
		long contextCounter = 10;
		double MinSupportValue = 0.8;
		
		
	      long sum = 0;
	      for (LongWritable val : values) {
	        sum += val.get();
	      }
	      if((double)(sum/contextCounter) >= MinSupportValue) {
	      context.write(key, new LongWritable(sum));
	      }
    }
}
