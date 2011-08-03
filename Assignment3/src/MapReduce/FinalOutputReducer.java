package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalOutputReducer extends
		Reducer<Text, TextTaggedValue, Text, Text> {
	/**
	 * Reduce. Emit all tags and probabilities for a word.
	 * 
	 * @param key The word.
	 * @param taggedValues Tags and their p(tag|word) values for the word.
	 * @param context The Hadoop context.
	 */
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		StringBuilder sb = new StringBuilder();
		for (TextTaggedValue taggedValue : taggedValues)
		{
			sb.append(" ");
			sb.append(taggedValue.getValue().getText().toString());
			sb.append(" ");
			sb.append(taggedValue.getValue().getValue().get());			
		}
		context.write(key, new Text(sb.toString()));
	}
}
