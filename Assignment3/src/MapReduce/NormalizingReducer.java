package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NormalizingReducer extends
		Reducer<Text, TextTaggedValue, Text, FloatWritable> {
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		List<TextFloatWritable> tags = new ArrayList<TextFloatWritable>();
		float sum = 0;
		for (TextTaggedValue taggedValue : taggedValues) {
			TextTaggedValue ttv = new TextTaggedValue(
					new Text(taggedValue.getTag()),
					new TextFloatWritable(
							new Text(taggedValue.getValue().getText()),
							new FloatWritable(taggedValue.getValue().getValue().get())));
			
			sum += ttv.getValue().getValue().get();
			tags.add(ttv.getValue());
		}
		
		for (TextFloatWritable tfw : tags) {
			float v = tfw.getValue().get() / sum;
			
			context.write(
				new Text(key.toString() + "-,-" + tfw.getText().toString() ),
				new FloatWritable(v));
		}
	}
}
