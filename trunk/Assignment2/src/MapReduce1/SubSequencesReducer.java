package MapReduce1;

/*
 * HadoopReducer.java
 *
 * Created on Apr 27, 2011, 8:01:55 PM
 */


import java.io.IOException;
import java.util.ArrayList;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author yarneo
 */
public class SubSequencesReducer extends Reducer<Text,UserWritable,Text,UserWritable> {

	public void reduce(Text key, Iterable<UserWritable> values,Context context) throws IOException, InterruptedException {
		long counter = 10000;
		double MinSupportValue = 0.8;
		
		int sum = 0;
		ArrayList<UserWritable> valueList = new ArrayList<UserWritable>();
		for (UserWritable val : values) {
			sum++;
			ContextsUserWritable cuw = new ContextsUserWritable((ContextsUserWritable)val.getContexts().get()[0]);
			ContextsUserWritable[] cuwArr = {cuw};
			valueList.add(new UserWritable(new LongWritable(val.getFrequency().get()),
					new UserArrayWritable(cuwArr)));
			
		}
		int i=0;
		long frequency = 0;
		ContextsUserWritable[] ctxts = new ContextsUserWritable[sum];
		for (UserWritable val : valueList) {
			frequency += val.getFrequency().get();
			ctxts[i] = (ContextsUserWritable)(val.getContexts().get()[0]);
			i++;
		}
		if((double)(frequency / counter) >= MinSupportValue) {
		UserWritable result = new UserWritable(new LongWritable(frequency),new UserArrayWritable(ctxts));	
		//System.out.println(context.getCounter(HadoopJob.Counters.INPUT_WORDS).getValue());
		context.write(key, result);
		}
	}
}