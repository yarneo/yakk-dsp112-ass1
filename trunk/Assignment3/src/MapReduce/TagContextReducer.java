
/*
 * SuppProbReducer.java
 *
 * Created on Jun 4, 2011, 3:51:12 PM
 */

package MapReduce;


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
public class TagContextReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("MapReduce.SuppProbReducer");

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // TODO code reducer logic here
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
