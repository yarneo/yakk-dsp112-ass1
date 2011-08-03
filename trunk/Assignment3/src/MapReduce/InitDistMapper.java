
/*
 * InitDistMapper.java
 *
 * Created on Jun 4, 2011, 1:52:57 PM
 */

package MapReduce;


import java.io.IOException;
import java.util.StringTokenizer;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class InitDistMapper extends Mapper<Text,LongWritable,Text,LongWritable> {
    /**
     * Map. Emit word, count for each word in n-gram record.
     * 
     * @param key The n-gram.
     * @param value The n-gram's count.
     * @param context The Hadoop context.
     */
	@Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(key.toString()); // Tokenize n-gram by whitespace.
		while (itr.hasMoreTokens()) {			
			context.write(new Text(itr.nextToken()), value); // Emit word from n-gram with n-gram's count.          
		}
    }
}
