
/*
 * InitDistReducer.java
 *
 * Created on Jun 4, 2011, 2:20:27 PM
 */

package MapReduce;


import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import dsp.tagger.Analysis;
import dsp.tagger.AnalysisException;
import dsp.tagger.TagDictionary;

public class InitDistReducer extends Reducer<Text,LongWritable,Text,DoubleWritable> {
	private static final Log LOG = LogFactory.getLog("InitDistReducer");
	
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    	TagDictionary tagger = TagDictionarySingleton.getInstance();    	

		long threshold = context.getConfiguration().getLong("threshold", -1);
		if(threshold == -1) {
			LOG.error("Error retrieving threshold");
			return;
		}
    	long frequency = 0;
    	for (LongWritable val : values) {
    		frequency += val.get();
    	}
    	if(frequency >= threshold) {
    		boolean uniform = context.getConfiguration().getBoolean("uniform", false);
    		    		
    		try {   			
    			for (Analysis<String> analysis : tagger.getTagsDistributionForWord(key.toString(), uniform)) {
    				String wordAndTag = key.toString() + "-,-" + analysis.getTag();
    				context.write(
    						new Text(wordAndTag),
    						new DoubleWritable(analysis.getProb()));
    			}
    		} catch (AnalysisException e) {
    			e.printStackTrace();
    			// TODO: handle this error
    		}	
    		
//    		float out = (float)0.5;
//    		String wordAndTag = key.toString() + "-,-" + "noun";
//    		context.write(new Text(wordAndTag), new FloatWritable(out));
//    		wordAndTag = key.toString() + "-,-" + "verb";
//    		context.write(new Text(wordAndTag), new FloatWritable(out));
    	}    
    }
}
