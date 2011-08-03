package MapReduce;

import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class ContextInfoMapper extends Mapper<Text,LongWritable,Text,Pair> {
	/**
	 * Map. Emit context, [word, count] for each context in each n-gram, count input record.
	 * 
	 * @param key N-gram string.
	 * @param value N-gram count.
	 * @param context The Hadoop context.
	 */
	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		String[] ngramWords = key.toString().split("\\s+");
		// For each n-gram word
		for(int i = 0; i < ngramWords.length; i++) {
			String outContext = "";
			String outWord = "";
			// The word is the current word and the context is all the other words.
			for(int j = 0; j < ngramWords.length; j++) {
				if(i == j) {
					outContext += "_____";
					outWord = ngramWords[j];
				} else {
					outContext += ngramWords[j];
				}
				if(j != (ngramWords.length - 1)) {
					outContext += " ";
				}
			}
			context.write(new Text(outContext), new Pair(new Text(outWord),value));
		}
	}
}
