
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import dsp.tagger.Analysis;
import dsp.tagger.AnalysisException;
import dsp.tagger.BGUTagDictionary;
import dsp.tagger.TagDictionary;

/**
 *
 * @author yarneo
 */
public class InitDistReducer extends Reducer<Text,LongWritable,Text,FloatWritable> {
	private static final Log LOG = LogFactory.getLog("InitDistReducer");
	
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//    	TagDictionary tagger = null;
//		try {
//			tagger = new BGUTagDictionary("lexicon.txt","known-bitmasks.txt","swmap.txt","non-count-nouns.txt");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
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
//    		 try {
//				for (Analysis<String> analysis : tagger.getTagsDistributionForWord(key.toString(),true)) 
//				        System.out.println("\t" + analysis.getTag() + ": " + analysis.getProb());
//			} catch (AnalysisException e) {
//				e.printStackTrace();
//			}
    		//get from dictionary distribution and write it.
    		//output should have <word,tag,probability>.
    		//Dont know whats wiser to make key and what to make value yet.
    		float out = (float)0.5;
    		String wordAndTag = key.toString() + "-,-" + "noun";
    		context.write(new Text(wordAndTag), new FloatWritable(out));
    		wordAndTag = key.toString() + "-,-" + "verb";
    		context.write(new Text(wordAndTag), new FloatWritable(out));
    	}
    }
}
