package MapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FinalOutputMapper extends
		Mapper<Text, DoubleWritable, Text, TextTaggedValue> {
	@Override
    protected void map(Text key, DoubleWritable value, Context context)
    		throws IOException, InterruptedException
	{
		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		Text tag = new Text(inputPath.getName().split("-")[0]);
		
		String[] tokens = key.toString().split("-,-");
		Text joinKey = new Text(tokens[0]);
		Text text = new Text(tokens[1]);
		
		TextTaggedValue v = new TextTaggedValue(tag, new TextDoubleWritable(text, value));
		
		context.write(joinKey, v);
	}
}
