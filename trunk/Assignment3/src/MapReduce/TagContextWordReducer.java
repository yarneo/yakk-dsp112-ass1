package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TagContextWordReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		float sum = 0;
		for (FloatWritable fw : values) {
			sum += fw.get();
		}
		context.write(key, new FloatWritable(sum));
	}

}
