
/*
 * SuppProbMapper.java
 *
 * Created on Jun 4, 2011, 3:50:54 PM
 */

package MapReduce;


import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author yarneo
 */
public class TagContextMapper extends Mapper<Text,LongWritable,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("MapReduce.SuppProbMapper");

    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {

    	throw new UnsupportedOperationException("Not supported yet.");
    }
}
