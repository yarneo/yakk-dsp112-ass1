package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NormalizingSumReducer extends
		Reducer<Text, TextDoubleWritable, Text, TextDoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<TextDoubleWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		double sum = 0;
		for (TextDoubleWritable tdw : values) {			
			sum += tdw.getValue().get();
		}		
		context.write(key, new TextDoubleWritable(new Text("sum"), new DoubleWritable(sum)));		
	}

}
