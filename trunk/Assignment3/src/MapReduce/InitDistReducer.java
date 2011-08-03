
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
	/**
	 * Reducer. Emit word-,-tag, p(tag|word) according to dictionary for each tag for each word that
	 *          meets the threshold.
	 *
	 * @param key The word.
	 * @param values The counts in each record for the word.
	 * @param context The Hadoop context.
	 */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    	TagDictionary tagger = TagDictionarySingleton.getInstance();    	

		long threshold = context.getConfiguration().getLong("threshold", -1);
		if(threshold == -1) {			
			LOG.error("Error retrieving threshold");
			return;
		}
    	long frequency = 0;
    	// Sum all appearances of the word.
    	for (LongWritable val : values) {
    		frequency += val.get();
    	}
    	// If the word meets the threshold, emit p(tag|word)'s for it.
    	if(frequency >= threshold) {
    		boolean uniform = context.getConfiguration().getBoolean("uniform", false);
    		    		
    		try {
    			for (Analysis<String> analysis : tagger.getTagsDistributionForWord(key.toString(), uniform)) {
    				String wordAndTag = key.toString() + "-,-" + analysis.getTag();
    				// Emit tag for word with probability from the dictionary's analysis.
    				context.write(
    						new Text(wordAndTag),
    						new DoubleWritable(analysis.getProb()));
    			}
    		} catch (AnalysisException e) {
    			e.printStackTrace();    			
    			// TODO: handle this error
    		}
    	}    
    }
}
