package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dsp.tagger.AnalysisException;
import dsp.tagger.TagDictionary;

public class ContextJoinReducer extends
		Reducer<Text, TextTaggedValue, Text, DoubleWritable> {
	
	private float allow(String tag, String word)
	{
		TagDictionary tagger = TagDictionarySingleton.getInstance();
		
		try {
			// TODO: "-" prefix in tags from getTagsForWord.
			if (tagger.getTagsForWord(word).contains(tag) || tagger.getTagsForWord(word).contains("-" + tag)) {
				return 1;
			} else {
				return 0;
			}
		} catch (AnalysisException ae) {
			ae.printStackTrace();
			return 0;
		}				
	}
	
	@Override
	public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) 
			throws IOException, InterruptedException 
	{
		List<TextDoubleWritable> tags = new ArrayList<TextDoubleWritable>();
		List<TextDoubleWritable> words = new ArrayList<TextDoubleWritable>();
		
		for (TextTaggedValue taggedValue : taggedValues) {
			// deep copy since Hadoop recycles Writables during the iteration.
			TextTaggedValue ttv = new TextTaggedValue(
					new Text(taggedValue.getTag()),
					new TextDoubleWritable(
							new Text(taggedValue.getValue().getText()),
							new DoubleWritable(taggedValue.getValue().getValue().get())));
			
			if (taggedValue.getTag().toString().equals("tag")) {
				tags.add(ttv.getValue());
			} else if (taggedValue.getTag().toString().equals("word")) {
				words.add(ttv.getValue());
			} else {
				// TODO: handle this case
				System.out.println("epic fail 4");
			}	
		}
		
		for (TextDoubleWritable tagContext : tags) {
			for (TextDoubleWritable contextWord : words) {
				
				
				String tag = tagContext.getText().toString();
				String word = contextWord.getText().toString();
								
				double f = tagContext.getValue().get() * contextWord.getValue().get() * allow(tag, word);
				
				context.write(
						new Text(tag + "-,-" + word),
						new DoubleWritable(f));
			}	
		}
	}

}
