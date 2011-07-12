package MapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ContextTaggingMapper extends
		Mapper<Text, FloatWritable, Text, TextTaggedValue> {
	@Override
    protected void map(Text key, FloatWritable value, Context context)
    		throws IOException, InterruptedException
	{
		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		Text tag = new Text(inputPath.getName().split("-")[0]);
		
		String[] tokens = key.toString().split("-,-");
		Text joinKey = new Text(tokens[1]);
		Text text = new Text(tokens[0]);
		
		TextTaggedValue v = new TextTaggedValue(tag, new TextFloatWritable(text, value));
		
		context.write(joinKey, v);
	}
}
