
/*
 * WordInfoMapper.java
 *
 * Created on Jun 6, 2011, 4:47:59 PM
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
public class WordInfoMapper extends Mapper<Text,LongWritable,Text,Pair> {

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
			context.write(new Text(outWord), new Pair(new Text(outStr),value));
		}
	}
}
