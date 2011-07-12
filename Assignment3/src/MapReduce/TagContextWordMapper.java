package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagContextWordMapper extends
		Mapper<Text, FloatWritable, Text, FloatWritable> {
	@Override
    protected void map(Text key, FloatWritable value, Context context)
    		throws IOException, InterruptedException
	{
		context.write(key, value);
    }
}
