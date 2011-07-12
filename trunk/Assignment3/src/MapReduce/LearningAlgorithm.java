package MapReduce;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import dsp.tagger.BGUTagDictionary;
import dsp.tagger.TagDictionary;

public class LearningAlgorithm {


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 11) {
			System.err.println(
					"Usage: learningalgo <threshold> <inCorpus> <outNgrams> <outInitDist> <outWord> <outContext> <outWordJoin> <outWordSum> <outContextJoin> <outContextSum> <outNormalize>");
			System.exit(3);
		}
		System.out.println(new Date().toString());
		
		long T = Long.parseLong(otherArgs[0]);
		Path corpusPath = new Path(otherArgs[1]);
		Path countedNgramsOutputPath = new Path(otherArgs[2]);
		Path initialDistributionOutputPath = new Path(otherArgs[3]);
		Path wordInfoOutputPath = new Path(otherArgs[4]);
		Path contextInfoOutputPath = new Path(otherArgs[5]);
		Path wordJoinOutputPath = new Path(otherArgs[6]);
		Path wordSumOutputPath = new Path(otherArgs[7]);
		Path contextJoinOutputPath = new Path(otherArgs[8]);
		Path contextSumOutputPath = new Path(otherArgs[9]);
		Path normalizeOutputPath = new Path(otherArgs[10]);
		
		boolean succeeded = false;

		Job countAndFormatJob = new Job(conf, "count and format");
		countAndFormatJob.setJarByClass(LearningAlgorithm.class);
		countAndFormatJob.setMapperClass(RowMapper.class);
		countAndFormatJob.setMapOutputKeyClass(Text.class);
		countAndFormatJob.setMapOutputValueClass(LongWritable.class);
		//countAndFormatJob.setReducerClass(RowReducer.class);
		countAndFormatJob.setReducerClass(LongSumReducer.class);
		FileInputFormat.setMinInputSplitSize(countAndFormatJob, 1024L*10000);
		FileInputFormat.setMaxInputSplitSize(countAndFormatJob, 1024L*20000);
		countAndFormatJob.setInputFormatClass(SequenceFileInputFormat.class);
		countAndFormatJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		countAndFormatJob.setOutputKeyClass(Text.class);
		countAndFormatJob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(countAndFormatJob, corpusPath);
		FileOutputFormat.setOutputPath(countAndFormatJob, countedNgramsOutputPath);

		succeeded = countAndFormatJob.waitForCompletion(true);

		if (!succeeded) {
			System.err.println("First job failed");
			System.exit(1);
		}

		conf.setLong("threshold", T);

		Job initialDistribution = new Job(conf, "initial distribution");
		initialDistribution.setJarByClass(LearningAlgorithm.class);
		initialDistribution.setMapperClass(InitDistMapper.class);
		initialDistribution.setMapOutputKeyClass(Text.class);
		initialDistribution.setMapOutputValueClass(LongWritable.class);
		initialDistribution.setReducerClass(InitDistReducer.class);
		initialDistribution.setInputFormatClass(SequenceFileInputFormat.class);
		initialDistribution.setOutputFormatClass(SeqOutInitDist.class);
		initialDistribution.setOutputKeyClass(Text.class);
		initialDistribution.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(initialDistribution, countedNgramsOutputPath);
		FileOutputFormat.setOutputPath(initialDistribution, initialDistributionOutputPath);

		succeeded = initialDistribution.waitForCompletion(true);

		if (!succeeded) {
			System.err.println("Second job failed");
			System.exit(2);
		}
		
		Job wordInfoJob = new Job(conf, "word info");
		wordInfoJob.setJarByClass(LearningAlgorithm.class);
		wordInfoJob.setMapperClass(WordInfoMapper.class);
		wordInfoJob.setMapOutputKeyClass(Text.class);
		wordInfoJob.setMapOutputValueClass(Pair.class);
		wordInfoJob.setReducerClass(WordInfoReducer.class);
		wordInfoJob.setInputFormatClass(SequenceFileInputFormat.class);
		wordInfoJob.setOutputFormatClass(SeqOutFormatWord.class);
		wordInfoJob.setOutputKeyClass(Text.class);
		wordInfoJob.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(wordInfoJob, countedNgramsOutputPath);
		FileOutputFormat.setOutputPath(wordInfoJob, wordInfoOutputPath);

		succeeded = wordInfoJob.waitForCompletion(true);

		if (!succeeded) {
			System.err.println("Third job failed");
			System.exit(3);
		}
		
		// TODO: word info and context info can run at the same time.

		Job contextInfoJob = new Job(conf, "context info");
		contextInfoJob.setJarByClass(LearningAlgorithm.class);
		contextInfoJob.setMapperClass(ContextInfoMapper.class);
		contextInfoJob.setMapOutputKeyClass(Text.class);
		contextInfoJob.setMapOutputValueClass(Pair.class);
		contextInfoJob.setReducerClass(ContextInfoReducer.class);
		contextInfoJob.setInputFormatClass(SequenceFileInputFormat.class);
		contextInfoJob.setOutputFormatClass(SeqOutFormatContext.class);
		contextInfoJob.setOutputKeyClass(Text.class);
		contextInfoJob.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(contextInfoJob, countedNgramsOutputPath);
		FileOutputFormat.setOutputPath(contextInfoJob, contextInfoOutputPath);

		succeeded = contextInfoJob.waitForCompletion(true);

		if (!succeeded) {
			System.err.println("Fourth job failed");
			System.exit(4);
		}
		
		for (int i = 0; i < 10; i++) {						
			Job wordJoinJob = new Job(conf, "word join");
			wordJoinJob.setJarByClass(LearningAlgorithm.class);
			wordJoinJob.setMapperClass(TaggingMapper.class);
			wordJoinJob.setMapOutputKeyClass(Text.class);
			wordJoinJob.setMapOutputValueClass(TextTaggedValue.class);
			wordJoinJob.setReducerClass(WordJoinReducer.class);
			wordJoinJob.setInputFormatClass(SequenceFileInputFormat.class);
			wordJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			wordJoinJob.setOutputKeyClass(Text.class);
			wordJoinJob.setOutputValueClass(FloatWritable.class);
			FileInputFormat.addInputPath(wordJoinJob, initialDistributionOutputPath);
			FileInputFormat.addInputPath(wordJoinJob, wordInfoOutputPath);
			FileOutputFormat.setOutputPath(wordJoinJob, wordJoinOutputPath);
			
			succeeded = wordJoinJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Fifth job failed");
				System.exit(5);
			}
			
			Job wordSumJob = new Job(conf, "word sum");
			wordSumJob.setJarByClass(LearningAlgorithm.class);
			wordSumJob.setMapperClass(TagContextWordMapper.class);
			wordSumJob.setMapOutputKeyClass(Text.class);
			wordSumJob.setMapOutputValueClass(FloatWritable.class);
			wordSumJob.setReducerClass(TagContextWordReducer.class);
			wordSumJob.setInputFormatClass(SequenceFileInputFormat.class);
			wordSumJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			wordSumJob.setOutputKeyClass(Text.class);
			wordSumJob.setOutputValueClass(FloatWritable.class);
			wordSumJob.setOutputFormatClass(SeqOutInitDist.class);
			FileInputFormat.addInputPath(wordSumJob, wordJoinOutputPath);
			FileOutputFormat.setOutputPath(wordSumJob, wordSumOutputPath);
			
			succeeded = wordSumJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Sixth job failed");
				System.exit(6);
			}
			
			Job contextJoinJob = new Job(conf, "context join");
			contextJoinJob.setJarByClass(LearningAlgorithm.class);
			contextJoinJob.setMapperClass(ContextTaggingMapper.class);
			contextJoinJob.setMapOutputKeyClass(Text.class);
			contextJoinJob.setMapOutputValueClass(TextTaggedValue.class);
			contextJoinJob.setReducerClass(ContextJoinReducer.class);
			contextJoinJob.setInputFormatClass(SequenceFileInputFormat.class);
			contextJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			contextJoinJob.setOutputKeyClass(Text.class);
			contextJoinJob.setOutputValueClass(FloatWritable.class);
			FileInputFormat.addInputPath(contextJoinJob, wordSumOutputPath);
			FileInputFormat.addInputPath(contextJoinJob, contextInfoOutputPath);
			FileOutputFormat.setOutputPath(contextJoinJob, contextJoinOutputPath);
			
			succeeded = contextJoinJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Seventh job failed");
				System.exit(7);
			}
			
			Job contextSumJob = new Job(conf, "context sum");
			contextSumJob.setJarByClass(LearningAlgorithm.class);
			contextSumJob.setMapperClass(TagContextWordMapper.class);
			contextSumJob.setMapOutputKeyClass(Text.class);
			contextSumJob.setMapOutputValueClass(FloatWritable.class);
			contextSumJob.setReducerClass(TagContextWordReducer.class);
			contextSumJob.setInputFormatClass(SequenceFileInputFormat.class);
			contextSumJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			contextSumJob.setOutputKeyClass(Text.class);
			contextSumJob.setOutputValueClass(FloatWritable.class);
			contextSumJob.setOutputFormatClass(SeqOutInitDist.class);
			FileInputFormat.addInputPath(contextSumJob, contextJoinOutputPath);
			FileOutputFormat.setOutputPath(contextSumJob, contextSumOutputPath);
			
			succeeded = contextSumJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Seventh job failed");
				System.exit(7);
			}
			
			Job normalizingJob = new Job(conf, "normalize");
			normalizingJob.setJarByClass(LearningAlgorithm.class);
			normalizingJob.setMapperClass(ContextTaggingMapper.class);
			normalizingJob.setMapOutputKeyClass(Text.class);
			normalizingJob.setMapOutputValueClass(TextTaggedValue.class);
			normalizingJob.setReducerClass(NormalizingReducer.class);
			normalizingJob.setInputFormatClass(SequenceFileInputFormat.class);
			normalizingJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			normalizingJob.setOutputFormatClass(SeqOutInitDist.class);
			normalizingJob.setOutputKeyClass(Text.class);
			normalizingJob.setOutputValueClass(FloatWritable.class);
			FileInputFormat.addInputPath(normalizingJob, contextSumOutputPath);
			FileOutputFormat.setOutputPath(normalizingJob, normalizeOutputPath);
			
			succeeded = normalizingJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Eighth job failed");
				System.exit(8);
			}
			
			if (!fs.delete(initialDistributionOutputPath, true)) {
				System.out.println("Error deleting initial distribution.");
			}			
			if (!fs.delete(wordJoinOutputPath, true)) {
				System.out.println("Error deleting word join.");		
			}
			if (!fs.delete(wordSumOutputPath, true)) {
				System.out.println("Error deleting word sum.");		
			}			
			if (!fs.delete(contextJoinOutputPath, true)) {
				System.out.println("Error deleting context join.");		
			}
			if (!fs.delete(contextSumOutputPath, true)) {
				System.out.println("Error deleting context sum.");		
			}
			
			if (!fs.rename(normalizeOutputPath, initialDistributionOutputPath)) {
				System.out.println("Error renaming normalize output.");
			}
		}

		
				
//		conf.set("mapred.textoutputformat.separator", ",");
//		Job joinJob = new Job(conf, "DataJoin");
//		joinJob.setJarByClass(LearningAlgorithm.class);
//		joinJob.setInputFormatClass(KeyTaggedValueTextInputFormat.class);
//		//joinJob.setOutputFormatClass(TextOutputFormat.class);
//		joinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
//		joinJob.setMapperClass(ReduceSideJoin.MapClass.class);
//		//joinJob.setReducerClass(ReduceSideJoin.ReduceClass.class);
//		FileInputFormat.addInputPath(joinJob, new Path(wordInfoOutputPath));
//		FileInputFormat.addInputPath(joinJob, new Path(initialDistributionOutputPath));
//		FileOutputFormat.setOutputPath(joinJob, new Path(tagContextOutputPath));
//		joinJob.setOutputKeyClass(Text.class);
//		joinJob.setOutputValueClass(TextTaggedValue.class);
//		//joinJob.setOutputValueClass(Text.class);
//		//joinJob.setMapOutputKeyClass(Text.class);
//		//joinJob.setMapOutputValueClass(TextTaggedValue.class);
//
//		succeeded = joinJob.waitForCompletion(true);
//
//		if (!succeeded) {
//			System.err.println("Fifth job failed");
//			System.exit(5);
//		}

		//		  Job job = new Job(conf, "join");
		//		  Path in = new Path(args[0]);
		//		  Path out = new Path(args[1]);
		//		  FileInputFormat.setInputPaths(job, in);
		//		  FileOutputFormat.setOutputPath(job, out);
		//		  job.setJobName("DataJoinByPattern");
		//		  job.setMapperClass(OuterJoinMapReduce.MapClass.class);
		//		  job.setReducerClass(OuterJoinMapReduce.Reduce.class);
		//		  job.setInputFormat(TextInputFormat.class);
		//		  job.setOutputFormat(TextOutputFormat.class);
		//		  job.setOutputKeyClass(Text.class);
		//		  job.setOutputValueClass(TaggedWritable.class);
		//		  job.set("mapred.textoutputformat.separator", ",");
		//		  JobClient.runJob(job);
		//		
		//		
		//		succeeded = contextInfoJob.waitForCompletion(true);
		//		
		//		if (!succeeded) {
		//			System.err.println("Fourth job failed");
		//			System.exit(2);
		//		}

		//		public int run(String[] args) throws Exception {
		//			  Configuration conf = getConf();
		//			  JobConf job = new JobConf(conf, ReduceSideJoinByPattern.class);
		//			  Path in = new Path(args[0]);
		//			  Path out = new Path(args[1]);
		//			  FileInputFormat.setInputPaths(job, in);
		//			  FileOutputFormat.setOutputPath(job, out);
		//			  job.setJobName("DataJoinByPattern");
		//			  job.setMapperClass(MapClass.class);
		//			  job.setReducerClass(Reduce.class);
		//			  job.setInputFormat(TextInputFormat.class);
		//			  job.setOutputFormat(TextOutputFormat.class);
		//			  job.setOutputKeyClass(Text.class);
		//			  job.setOutputValueClass(TaggedWritable.class);
		//			  job.set("mapred.textoutputformat.separator", ",");
		//			  JobClient.runJob(job);
		//			  return 0;
		//			}
		//			 
		//			  public static void main(String[] args) throws Exception {
		//			    int res = ToolRunner.run(new Configuration(),
		//			    new ReduceSideJoin(),
		//			    args);
		//			    System.exit(res);
		//			  }

		//		conf.setLong(
		//				"fivegrams",
		//				countAndFormatJob.getCounters().findCounter(ContextsCounters.FIVEGRAMS_COUNTER).getValue());	
		//		conf.setFloat("minsup", Float.parseFloat(otherArgs[5]));	
		//		
		//		Job subsequencesJob = new Job(conf, "subsequences");
		//		subsequencesJob.setJarByClass(ContextsMain.class);
		//		subsequencesJob.setMapperClass(SubSequencesMapper.class);
		//		subsequencesJob.setMapOutputKeyClass(Text.class);
		//		subsequencesJob.setMapOutputValueClass(UserWritable.class);
		//		subsequencesJob.setReducerClass(SubSequencesReducer.class);
		//		subsequencesJob.setInputFormatClass(SequenceFileInputFormat.class);
		//		subsequencesJob.setOutputFormatClass(SequenceFileOutputFormat.class);		
		//		subsequencesJob.setOutputKeyClass(Text.class);
		//		subsequencesJob.setOutputValueClass(UserWritable.class);
		//		FileInputFormat.addInputPath(subsequencesJob, new Path(otherArgs[1]));
		//		FileOutputFormat.setOutputPath(subsequencesJob, new Path(otherArgs[2]));
		//		
		//	
		//		succeeded = subsequencesJob.waitForCompletion(true);
		//		
		//		if (!succeeded) {
		//			System.err.println("Second job failed");
		//			System.exit(2);
		//		}

		//		conf.setLong(
		//				"contextss",
		//				subsequencesJob.getCounters().findCounter(ContextsCounters.CONTEXTS_COUNTER).getValue());
		//		conf.setFloat("minrelfreq", Float.parseFloat(otherArgs[6]));
		//		
		//		Job contextsJob = new Job(conf, "contexts");
		//		contextsJob.setJarByClass(ContextsMain.class);
		//		contextsJob.setMapperClass(ContextsMapper.class);
		//		contextsJob.setMapOutputKeyClass(Text.class);
		//		contextsJob.setMapOutputValueClass(LongWritable.class);
		//		contextsJob.setReducerClass(ContextsReducer.class);
		//		contextsJob.setInputFormatClass(SequenceFileInputFormat.class);
		//		contextsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		//		contextsJob.setOutputKeyClass(Text.class);
		//		contextsJob.setOutputValueClass(LongWritable.class);
		//		FileInputFormat.addInputPath(contextsJob, new Path(otherArgs[2]));
		//		FileOutputFormat.setOutputPath(contextsJob, new Path(otherArgs[3]));
		//		
		//		
		//		succeeded = contextsJob.waitForCompletion(true);
		//		
		//		if (!succeeded) {
		//			System.err.println("Third job failed");
		//			System.exit(2);
		//		}

		//		conf.set("mapred.textoutputformat.separator", " , ");
		//		
		//		Job sortJob = new Job(conf, "sort");
		//		sortJob.setJarByClass(ContextsMain.class);
		//		sortJob.setMapperClass(SortMapper.class);
		//		sortJob.setMapOutputKeyClass(LongWritable.class);
		//		sortJob.setMapOutputValueClass(Text.class);
		//		sortJob.setSortComparatorClass(org.apache.hadoop.io.LongWritable.DecreasingComparator.class);
		//		sortJob.setReducerClass(SortReducer.class);
		//		sortJob.setNumReduceTasks(1);
		//		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		//		sortJob.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		//		sortJob.setOutputKeyClass(Text.class);
		//		sortJob.setOutputValueClass(LongWritable.class);
		//		FileInputFormat.addInputPath(sortJob, new Path(otherArgs[3]));
		//		FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[4]));
		//		
		//		
		//		succeeded = sortJob.waitForCompletion(true);
		//		
		//		if (!succeeded) {
		//			System.err.println("Third job failed");
		//			System.exit(2);
		//		}
		//		
		//		System.out.println(new Date().toString());
	}

}
