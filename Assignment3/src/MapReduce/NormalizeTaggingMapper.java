package MapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NormalizeTaggingMapper extends
		Mapper<Text, TextDoubleWritable, Text, TextTaggedValue> {
	/**
	 * Map. Input key, value records are emitted as tag-,-key, [tag, value].
	 * 
	 * @param key The record key.
	 * @param value The record value.
	 * @param context The Hadoop context.
	 */
	@Override
    protected void map(Text key, TextDoubleWritable value, Context context)
    		throws IOException, InterruptedException
	{
		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		String tag = inputPath.getName().split("-")[0];
		
		TextTaggedValue v = new TextTaggedValue(new Text(tag), value);
		
		Text outKey = new Text(tag.toString() + "-,-" + key);
		context.write(outKey, v);
	}
}
