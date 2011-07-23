package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ContextSumReducer extends
		Reducer<Text, DoubleWritable, Text, TextDoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		double sum = 0;
		for (DoubleWritable dw : values) {			
			sum += dw.get();
		}
		
		String[] tokens = key.toString().split("-,-");
		Text tag = new Text(tokens[0]);
		Text word = new Text(tokens[1]);
		context.write(word, new TextDoubleWritable(tag, new DoubleWritable(sum)));		
	}

}
