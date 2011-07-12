package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordJoinReducer extends
		Reducer<Text, TextTaggedValue, Text, FloatWritable> {
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		// key is word
		// taggedValues has values with tag "tag" which are p(tag|word) for various tags of word in key.
		// taggedValues has values with tag "word" which are p(word|context) for various contexts of word in key.
		// for each tag|word
		// .. emit word, tag|context as tag|word * word|context for each word|context
		
		List<TextFloatWritable> tags = new ArrayList<TextFloatWritable>();
		List<TextFloatWritable> contexts = new ArrayList<TextFloatWritable>();
		
		for (TextTaggedValue taggedValue : taggedValues)
		{
			// deep copy since Hadoop recycles Writables during the iteration.
			TextTaggedValue ttv = new TextTaggedValue(
					new Text(taggedValue.getTag()),
					new TextFloatWritable(
							new Text(taggedValue.getValue().getText()),
							new FloatWritable(taggedValue.getValue().getValue().get())));
			
			if (taggedValue.getTag().toString().equals("tag")) {
				tags.add(ttv.getValue());				
			} else if (taggedValue.getTag().toString().equals("word")) {
				contexts.add(ttv.getValue());				
			} else {
				// TODO: handle this case
				System.out.println("epic fail 3");
			}			
		}		
		
		for (TextFloatWritable tagWord : tags) {
			for (TextFloatWritable wordContext : contexts) {
				float f = tagWord.getValue().get() * wordContext.getValue().get();
				
				context.write(
						new Text(tagWord.getText().toString() + "-,-" + wordContext.getText().toString()),
						new FloatWritable(f));
			}	
		}
	}

}
