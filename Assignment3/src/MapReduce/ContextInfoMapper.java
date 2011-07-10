
/*
 * ContextInfoMapper.java
 *
 * Created on Jun 6, 2011, 4:47:42 PM
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
public class ContextInfoMapper extends Mapper<Text,LongWritable,Text,Pair> {
	// The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
	// private static final Log LOG = LogFactory.getLog("MapReduce.ContextInfoMapper");

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		String[] strArr = key.toString().split("\\s+");
		for(int i=0;i<strArr.length;i++) {
			String outStr = "";
			String outWord = "";
			for(int j=0;j<strArr.length;j++) {
				if(i==j) {
					outStr += "_____";
					outWord = strArr[j];
				}
				else {
					outStr += strArr[j];
				}
				if(j != (strArr.length-1)) {
					outStr += " ";
				}
			}
			context.write(new Text(outStr), new Pair(new Text(outWord),value));
		}
	}
}
