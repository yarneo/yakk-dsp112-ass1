package MapReduce1;

/*
 * SortMapper.java
 *
 * Created on May 3, 2011, 6:24:43 PM
 */


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
public class SortMapper extends Mapper<Text,LongWritable,LongWritable,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("SortMapper");

    @Override
    protected void map(Text key, LongWritable value, Context context)
                    throws IOException, InterruptedException {
//	String[] strArr = value.toString().split("\\s+");
//	String keyStr = "";
//	long valueLong = 0;
//	for(int i=0;i<(strArr.length-1);i++) {
//		if(i == 0) {
//			keyStr+=strArr[i];
//		}
//		else{
//	keyStr+= " " + strArr[i];	
//		}
//	}
//	valueLong = Long.parseLong(strArr[strArr.length-1]);
	
	context.write(value, key);
	
    }
}
