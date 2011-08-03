package MapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TaggingMapper extends
		Mapper<Text, DoubleWritable, Text, TextTaggedValue> {
	/**
	 * Map. Tag records by input data source and groups by first part of key.
	 * 
	 * Let key be k1-,-k2. Then for key, value, the record tag-,-k1, [tag, [k2, value]] is emitted,
	 * where tag is the prefix of the input file for the input record.
	 * 
	 * @param key A key of the form k1-,-k2.
	 * @param value A value.
	 * @param context The Hadoop context.
	 */
	@Override
    protected void map(Text key, DoubleWritable value, Context context)
    		throws IOException, InterruptedException
	{
		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		String tag = inputPath.getName().split("-")[0];		
		
		String[] tokens = key.toString().split("-,-");
		String joinKey = tokens[0];
		Text text = new Text(tokens[1]);
		
		TextTaggedValue v = new TextTaggedValue(new Text(tag), new TextDoubleWritable(text, value));
		
		Text outKey = new Text(tag.toString() + "-,-" + joinKey);
		context.write(outKey, v);
	}
}
