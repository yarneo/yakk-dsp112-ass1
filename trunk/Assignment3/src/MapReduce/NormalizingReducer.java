package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NormalizingReducer extends
		Reducer<Text, TextTaggedValue, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		List<TextDoubleWritable> tags = new ArrayList<TextDoubleWritable>();
		float sum = 0;
		for (TextTaggedValue taggedValue : taggedValues) {
			TextTaggedValue ttv = new TextTaggedValue(
					new Text(taggedValue.getTag()),
					new TextDoubleWritable(
							new Text(taggedValue.getValue().getText()),
							new DoubleWritable(taggedValue.getValue().getValue().get())));
			
			sum += ttv.getValue().getValue().get();
			tags.add(ttv.getValue());
		}
		
		if (sum != 0) {
			for (TextDoubleWritable tfw : tags) {
				double v = tfw.getValue().get() / sum;
				
				if (v != 0) {
					context.write(
							new Text(key.toString() + "-,-" + tfw.getText().toString()),
							new DoubleWritable(v));	
				}				
			}	
		}
		
	}
}
