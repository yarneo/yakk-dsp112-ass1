package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NormalizingSumMapper extends
		Mapper<Text, TextDoubleWritable, Text, TextDoubleWritable>
{
	@Override
    protected void map(Text key, TextDoubleWritable value, Context context)
    		throws IOException, InterruptedException
	{
		context.write(key,  value);
	}

}
