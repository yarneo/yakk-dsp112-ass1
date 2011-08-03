package MapReduce;

import java.io.IOException;
//import java.io.PrintStream;
import java.util.ArrayList;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class WordInfoReducer extends Reducer<Text,Pair,Text,DoubleWritable> {
	/**
	 * Reducer. For each word and context input record, emit p(context|word) output record.
	 * 
	 * @param key The word.
	 * @param values Contexts and counts for the word.
	 * @param context The Hadoop context.
	 */
    @Override
    protected void reduce(Text key, Iterable<Pair> values, Context context) 
    		throws IOException, InterruptedException {
    	long count_of_word = 0;
		ArrayList<Pair> valueList = new ArrayList<Pair>();
        for(Pair val : values) {
        	count_of_word += val.getNum().get();
        	valueList.add(new Pair(new Text(val.getText().toString()),new LongWritable(val.getNum().get())));
        }
        for(Pair val : valueList) {
        	// Key: word-,-context   Value: count(word in context) / count(word) = p(context|word)
        	String outStr = key.toString() + "-,-" + val.getText().toString();
        	double outFloat = ((double)val.getNum().get() / count_of_word);

        	context.write(new Text(outStr), new DoubleWritable(outFloat));
        }
    }
}
