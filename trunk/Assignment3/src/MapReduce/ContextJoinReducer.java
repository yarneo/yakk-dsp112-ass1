package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ContextJoinReducer extends
		Reducer<Text, TextTaggedValue, Text, DoubleWritable> {	
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{		
		List<TextDoubleWritable> tags = new ArrayList<TextDoubleWritable>();
		
		for (TextTaggedValue taggedValue : taggedValues) {			
			if (taggedValue.getTag().toString().equals("tag")) {
				tags.add(new TextDoubleWritable(
						new Text(taggedValue.getValue().getText()),
						new DoubleWritable(taggedValue.getValue().getValue().get())));
			} else if (taggedValue.getTag().toString().equals("word")) {				
				TextDoubleWritable contextWord = taggedValue.getValue();

				for (TextDoubleWritable tagContext : tags) {
					String tag = tagContext.getText().toString();
					String word = contextWord.getText().toString();

					double f = 
						tagContext.getValue().get() * contextWord.getValue().get() * Common.allow(tag, word);

					if (f != 0) {
						context.write(
								new Text(tag + "-,-" + word),
								new DoubleWritable(f));
					}
				}
			} else {
				// TODO: handle this case
				System.out.println("epic fail 4");
			}
		}
	}
}
