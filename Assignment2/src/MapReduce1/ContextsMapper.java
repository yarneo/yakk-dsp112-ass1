
/*
 * ContextsMapper.java
 *
 * Created on Apr 30, 2011, 3:51:55 PM
 */

package MapReduce1;


import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import MapReduce1.ContextsUserWritable;
import MapReduce1.UserWritable;

/**
 *
 * @author yarneo
 */
public class ContextsMapper extends Mapper<Text,Object,Text,LongWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("MapReduce2.ContextsMapper");

    @Override
    protected void map(Text key, Object value, Context context)
                    throws IOException, InterruptedException {
    UserWritable uw = new UserWritable((UserWritable)value);
    for(int i=0;i<uw.getContexts().get().length;i++) {
    	ContextsUserWritable cuw = new ContextsUserWritable((ContextsUserWritable)uw.getContexts().get()[i]);
    	if(!cuw.getContext().toString().isEmpty()) {
    	context.write(cuw.getContext(), cuw.getFrequency());
    	}
    }
    }
}
