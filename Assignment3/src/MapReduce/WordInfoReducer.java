
/*
 * ContextInfoReducer.java
 *
 * Created on Jun 6, 2011, 4:48:14 PM
 */

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

/**
 *
 * @author yarneo
 */
public class WordInfoReducer extends Reducer<Text,Pair,Text,DoubleWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("MapReduce.ContextInfoReducer");

    @Override
    protected void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
    	long count_of_word = 0;
		ArrayList<Pair> valueList = new ArrayList<Pair>();
        for(Pair val : values) {
        	count_of_word += val.getNum().get();
        	valueList.add(new Pair(new Text(val.getText().toString()),new LongWritable(val.getNum().get())));
        }
        for(Pair val : valueList) {
        	//Key: context /tab word   Value: count(word in context) / count(word) = p(context|word)
        	//String outStr = val.getText().toString() + "-,-" + key.toString();
        	String outStr =key.toString() + "-,-" + val.getText().toString();
        	double outFloat = ((double)val.getNum().get() / count_of_word);
//        	 String unicodeMessage =
//        		 "WORDINFOREDUCER: " + outStr + " " + outFloat;
//
//        		    PrintStream out = new PrintStream(System.out, true, "UTF-8");
//        		    out.println(unicodeMessage);
        	context.write(new Text(outStr), new DoubleWritable(outFloat));
        }
    }
}
