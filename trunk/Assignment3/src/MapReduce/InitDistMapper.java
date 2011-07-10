
/*
 * InitDistMapper.java
 *
 * Created on Jun 4, 2011, 1:52:57 PM
 */

package MapReduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author yarneo
 */
public class InitDistMapper extends Mapper<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
    	
		StringTokenizer itr = new StringTokenizer(key.toString());
		while (itr.hasMoreTokens()) {
			context.write(new Text(itr.nextToken()), value);          
		}
    }
}
