package MapReduce1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ContextsMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: contextsmain <in> <out1> <out>");
			System.exit(3);
		}
		
		Job countAndFormatJob = new Job(conf, "count and format");
		countAndFormatJob.setJarByClass(ContextsMain.class);
		countAndFormatJob.setMapperClass(RowMapper.class);
		countAndFormatJob.setMapOutputKeyClass(Text.class);
		countAndFormatJob.setMapOutputValueClass(LongWritable.class);
		countAndFormatJob.setInputFormatClass(SequenceFileInputFormat.class);
		countAndFormatJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		countAndFormatJob.setOutputKeyClass(Text.class);
		countAndFormatJob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(countAndFormatJob, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(countAndFormatJob, new Path(otherArgs[1]));
		
		boolean succeeded = countAndFormatJob.waitForCompletion(true);
		
		if (!succeeded) {
			System.err.println("First job failed");
			System.exit(1);
		}
		
		Job subsequencesJob = new Job(conf, "subsequences");
		subsequencesJob.setJarByClass(ContextsMain.class);
		subsequencesJob.setMapperClass(SubSequencesMapper.class);
		subsequencesJob.setMapOutputKeyClass(Text.class);
		subsequencesJob.setMapOutputValueClass(UserWritable.class);
		subsequencesJob.setInputFormatClass(SequenceFileInputFormat.class);
		subsequencesJob.setOutputFormatClass(SequenceFileOutputFormat.class);		
		subsequencesJob.setOutputKeyClass(Text.class);
		subsequencesJob.setOutputValueClass(UserWritable.class);
		FileInputFormat.addInputPath(subsequencesJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(subsequencesJob, new Path(otherArgs[2]));
		
		conf.setLong(
				"fivegrams",
				countAndFormatJob.getCounters().findCounter(ContextsCounters.FIVEGRAMS_COUNTER).getValue());		
				
		succeeded = subsequencesJob.waitForCompletion(true);
		
		if (!succeeded) {
			System.err.println("Second job failed");
			System.exit(2);
		}
	}

}
