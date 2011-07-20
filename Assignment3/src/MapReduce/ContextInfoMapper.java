package MapReduce;

import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class ContextInfoMapper extends Mapper<Text,LongWritable,Text,Pair> {
	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		String[] ngramWords = key.toString().split("\\s+");
		for(int i = 0; i < ngramWords.length; i++) {
			String outContext = "";
			String outWord = "";
			for(int j = 0; j < ngramWords.length; j++) {
				if(i == j) {
					outContext += "_____";
					outWord = ngramWords[j];
				} else {
					outContext += ngramWords[j];
				}
				if(j != (ngramWords.length - 1)) {
					outContext += " ";
				}
			}
			context.write(new Text(outContext), new Pair(new Text(outWord),value));
		}
	}
}
