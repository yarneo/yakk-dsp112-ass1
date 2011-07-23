package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordJoinReducer extends
		Reducer<Text, TextTaggedValue, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		// key is word
		// taggedValues has values with tag "tag" which are p(tag|word) for various tags of word in key.
		// taggedValues has values with tag "context" which are p(word|context) for various contexts of word in key.
		// for each tag|word
		// .. emit word, tag|context as tag|word * word|context for each word|context		
		List<TextDoubleWritable> tags = new ArrayList<TextDoubleWritable>();
			
		for (TextTaggedValue taggedValue : taggedValues)
		{			
			if (taggedValue.getTag().toString().equals("tag")) {				
				tags.add(new TextDoubleWritable(
						new Text(taggedValue.getValue().getText()),
						new DoubleWritable(taggedValue.getValue().getValue().get())));
			} else if (taggedValue.getTag().toString().equals("context")) {				
				TextDoubleWritable wordContext = taggedValue.getValue();
				
				for (TextDoubleWritable tagWord : tags) {				
					double f = tagWord.getValue().get() * wordContext.getValue().get();
					
					if (f != 0) {
						context.write(
								new Text(tagWord.getText().toString() + "-,-" + wordContext.getText().toString()),
								new DoubleWritable(f));	
					}
				}
			} else {
				// TODO: handle this error.
				System.out.println("epic fail 3");
			}
		}		
	}
}
