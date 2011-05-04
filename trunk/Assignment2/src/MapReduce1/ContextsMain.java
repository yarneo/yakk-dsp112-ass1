package MapReduce1;
import java.util.Date;

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
		if (otherArgs.length != 7) {
			System.err.println("Usage: contextsmain <in> <out1> <out2> <out3> <out> <minsupport> <minrelativefrequency>");
			System.exit(3);
		}
		System.out.println(new Date().toString());
		
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
		
		conf.setLong(
				"fivegrams",
				countAndFormatJob.getCounters().findCounter(ContextsCounters.FIVEGRAMS_COUNTER).getValue());		
		conf.setFloat("minsup", Float.parseFloat(otherArgs[5]));	
		
		Job subsequencesJob = new Job(conf, "subsequences");
		subsequencesJob.setJarByClass(ContextsMain.class);
		subsequencesJob.setMapperClass(SubSequencesMapper.class);
		subsequencesJob.setMapOutputKeyClass(Text.class);
		subsequencesJob.setMapOutputValueClass(UserWritable.class);
		subsequencesJob.setReducerClass(SubSequencesReducer.class);
		subsequencesJob.setInputFormatClass(SequenceFileInputFormat.class);
		subsequencesJob.setOutputFormatClass(SequenceFileOutputFormat.class);		
		subsequencesJob.setOutputKeyClass(Text.class);
		subsequencesJob.setOutputValueClass(UserWritable.class);
		FileInputFormat.addInputPath(subsequencesJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(subsequencesJob, new Path(otherArgs[2]));
		
	
		succeeded = subsequencesJob.waitForCompletion(true);
		
		if (!succeeded) {
			System.err.println("Second job failed");
			System.exit(2);
		}
		
		conf.setLong(
				"contextss",
				subsequencesJob.getCounters().findCounter(ContextsCounters.CONTEXTS_COUNTER).getValue());
		conf.setFloat("minrelfreq", Float.parseFloat(otherArgs[6]));
		
		Job contextsJob = new Job(conf, "contexts");
		contextsJob.setJarByClass(ContextsMain.class);
		contextsJob.setMapperClass(ContextsMapper.class);
		contextsJob.setMapOutputKeyClass(Text.class);
		contextsJob.setMapOutputValueClass(LongWritable.class);
		contextsJob.setReducerClass(ContextsReducer.class);
		contextsJob.setInputFormatClass(SequenceFileInputFormat.class);
		contextsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		contextsJob.setOutputKeyClass(Text.class);
		contextsJob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(contextsJob, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(contextsJob, new Path(otherArgs[3]));
		
		
		succeeded = contextsJob.waitForCompletion(true);
		
		if (!succeeded) {
			System.err.println("Third job failed");
			System.exit(2);
		}
	
		conf.set("mapred.textoutputformat.separator", " , ");
		
		Job sortJob = new Job(conf, "sort");
		sortJob.setJarByClass(ContextsMain.class);
		sortJob.setMapperClass(SortMapper.class);
		sortJob.setMapOutputKeyClass(LongWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		sortJob.setSortComparatorClass(org.apache.hadoop.io.LongWritable.DecreasingComparator.class);
		sortJob.setReducerClass(SortReducer.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		sortJob.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(sortJob, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[4]));
		
		
		succeeded = sortJob.waitForCompletion(true);
		
		if (!succeeded) {
			System.err.println("Third job failed");
			System.exit(2);
		}
		
		System.out.println(new Date().toString());
	}

}
