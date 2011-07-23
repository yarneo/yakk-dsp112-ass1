package MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NormalizingReducer extends
		Reducer<Text, TextTaggedValue, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		String[] tokens = key.toString().split("-,-");
		String word = tokens[1];
		
		double sum = 0;
		for (TextTaggedValue taggedValue : taggedValues) {
			if (taggedValue.getTag().toString().equals("tag")) {				
				sum = taggedValue.getValue().getValue().get();
			} else {
				String tag = taggedValue.getValue().getText().toString();
				double v = taggedValue.getValue().getValue().get() / sum;
				
				if (v != 0) {
					context.write(new Text(word + "-,-" + tag), new DoubleWritable(v));
				}				
			}
		}
	}
}
