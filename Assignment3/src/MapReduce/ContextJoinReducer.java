package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ContextJoinReducer extends
		Reducer<Text, TextTaggedValue, Text, FloatWritable> {
	
	private float allow(String tag, String word)
	{
		return 1; // TODO: implement with BGUTagDictionary
	}
	
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		List<TextFloatWritable> tags = new ArrayList<TextFloatWritable>();
		List<TextFloatWritable> words = new ArrayList<TextFloatWritable>();
		
		for (TextTaggedValue taggedValue : taggedValues) {
			// deep copy since Hadoop recycles Writables during the iteration.
			TextTaggedValue ttv = new TextTaggedValue(
					new Text(taggedValue.getTag()),
					new TextFloatWritable(
							new Text(taggedValue.getValue().getText()),
							new FloatWritable(taggedValue.getValue().getValue().get())));
			
			if (taggedValue.getTag().toString().equals("tag")) {
				tags.add(ttv.getValue());
			} else if (taggedValue.getTag().toString().equals("context")) {
				words.add(ttv.getValue());
			} else {
				// TODO: handle this case
				System.out.println("epic fail 4");
			}	
		}
		
		for (TextFloatWritable tagContext : tags) {
			for (TextFloatWritable contextWord : words) {
				
				
				String tag = tagContext.getText().toString();
				String word = contextWord.getText().toString();
								
				float f = tagContext.getValue().get() * contextWord.getValue().get() * allow(tag, word);;
				
				context.write(
						new Text(tag + "-,-" + word),
						new FloatWritable(f));
			}	
		}
	}

}
