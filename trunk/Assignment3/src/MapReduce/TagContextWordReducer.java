package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TagContextWordReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		double sum = 0;
		for (DoubleWritable dw : values) {
			sum += dw.get();
		}
		context.write(key, new DoubleWritable(sum));
	}

}
