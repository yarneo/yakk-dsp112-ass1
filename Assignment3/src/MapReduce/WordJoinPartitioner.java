package MapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordJoinPartitioner extends Partitioner<Text, TextTaggedValue> {
	static HashPartitioner<Text, TextTaggedValue> hp = new HashPartitioner<Text, TextTaggedValue>();

	@Override
	public int getPartition(Text key, TextTaggedValue value, int numPartitions) {
		// Partition according to non-tag portion of the key.
		String[] tokens = key.toString().split("-,-");
		Text realKey = new Text(tokens[1]);		
		return hp.getPartition(realKey, value, numPartitions);
	}
}
