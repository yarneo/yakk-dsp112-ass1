package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagContextWordMapper extends
		Mapper<Text, DoubleWritable, Text, DoubleWritable> {
	/**
	 * Map. Identity mapper.
	 * 
	 * @param key A text key.
	 * @param value A double value.
	 * @param context The Hadoop context.
	 */
	@Override	
    protected void map(Text key, DoubleWritable value, Context context)
    		throws IOException, InterruptedException
	{
		context.write(key, value);
    }
}
