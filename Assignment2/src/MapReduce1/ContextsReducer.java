
/*
 * ContextsReducer.java
 *
 * Created on Apr 30, 2011, 3:52:13 PM
 */

package MapReduce1;


import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author yarneo
 */
public class ContextsReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	private static final Log LOG = LogFactory.getLog("ContextsReducer");

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                        throws IOException, InterruptedException {
		long counter = context.getConfiguration().getLong("contextss", -1);
		if (counter == -1) {
			LOG.error("Error retrieving counter");
			return;
		}
		double MinSupportValue = 0.005;
		
		
	      long sum = 0;
	      for (LongWritable val : values) {
	        sum += val.get();
	      }
	      //LOG.info("COUNTER ISSSSSS:" + counter + " SUM ISSSSS:" + sum);
	      if((double)((double)sum / counter) >= MinSupportValue) {
	      context.write(key, new LongWritable(sum));
	      }
    }
}
