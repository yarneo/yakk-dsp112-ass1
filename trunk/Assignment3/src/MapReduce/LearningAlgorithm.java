package MapReduce;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class LearningAlgorithm {


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 14) {
			System.err.println(
					"Usage: learningalgo <threshold> <inCorpus> <outNgrams> <outInitDist> <outWord> <outContext> <outWordJoin> <outWordSum> <outContextJoin> <outContextSum> <outNormalizeSum> <outNormalize> <out> <initialdist>");
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
		Path normalizeSumPath = new Path(otherArgs[10]);
		Path normalizeOutputPath = new Path(otherArgs[11]);
		Path finalOutputPath = new Path(otherArgs[12]);
		boolean uniform;
		if(Integer.parseInt(otherArgs[13]) == 1) {
			uniform = false;
		} else {
			uniform = true;
		}		
		
		boolean succeeded = false;

		Job countAndFormatJob = new Job(conf, "count and format");
		countAndFormatJob.setJarByClass(LearningAlgorithm.class);
		countAndFormatJob.setMapperClass(RowMapper.class);
		countAndFormatJob.setMapOutputKeyClass(Text.class);
		countAndFormatJob.setMapOutputValueClass(LongWritable.class);
		//countAndFormatJob.setReducerClass(RowReducer.class);
		countAndFormatJob.setReducerClass(LongSumReducer.class);
		countAndFormatJob.setCombinerClass(LongSumReducer.class);
		//FileInputFormat.setMinInputSplitSize(countAndFormatJob, 1024L*10000);
		//FileInputFormat.setMaxInputSplitSize(countAndFormatJob, 1024L*20000);
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

		conf.setBoolean("uniform", uniform);
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
		initialDistribution.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(initialDistribution, countedNgramsOutputPath);
		FileOutputFormat.setOutputPath(initialDistribution, initialDistributionOutputPath);
		
		initialDistribution.submit();
				
		Job wordInfoJob = new Job(conf, "word info");
		wordInfoJob.setJarByClass(LearningAlgorithm.class);
		wordInfoJob.setMapperClass(WordInfoMapper.class);
		wordInfoJob.setMapOutputKeyClass(Text.class);
		wordInfoJob.setMapOutputValueClass(Pair.class);
		wordInfoJob.setReducerClass(WordInfoReducer.class);
		wordInfoJob.setInputFormatClass(SequenceFileInputFormat.class);
		wordInfoJob.setOutputFormatClass(SeqOutFormatWord.class);
		wordInfoJob.setOutputKeyClass(Text.class);
		wordInfoJob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(wordInfoJob, countedNgramsOutputPath);
		FileOutputFormat.setOutputPath(wordInfoJob, wordInfoOutputPath);
		
		wordInfoJob.submit();		

		Job contextInfoJob = new Job(conf, "context info");
		contextInfoJob.setJarByClass(LearningAlgorithm.class);
		contextInfoJob.setMapperClass(ContextInfoMapper.class);
		contextInfoJob.setMapOutputKeyClass(Text.class);
		contextInfoJob.setMapOutputValueClass(Pair.class);
		contextInfoJob.setReducerClass(ContextInfoReducer.class);
		contextInfoJob.setInputFormatClass(SequenceFileInputFormat.class);
		contextInfoJob.setOutputFormatClass(SeqOutFormatContext.class);
		contextInfoJob.setOutputKeyClass(Text.class);
		contextInfoJob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(contextInfoJob, countedNgramsOutputPath);
		FileOutputFormat.setOutputPath(contextInfoJob, contextInfoOutputPath);
		
		succeeded = initialDistribution.waitForCompletion(true);

		if (!succeeded) {
			System.err.println("Second job failed");
			System.exit(2);
		}
		
		succeeded = wordInfoJob.waitForCompletion(true);

		if (!succeeded) {
			System.err.println("Third job failed");
			System.exit(3);
		}

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
			wordJoinJob.setPartitionerClass(WordJoinPartitioner.class);
			wordJoinJob.setGroupingComparatorClass(WordJoinGroupComparator.class);
			wordJoinJob.setSortComparatorClass(WordJoinSortComparator.class);
			wordJoinJob.setReducerClass(WordJoinReducer.class);
			wordJoinJob.setInputFormatClass(SequenceFileInputFormat.class);
			wordJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			wordJoinJob.setOutputKeyClass(Text.class);
			wordJoinJob.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(wordJoinJob, initialDistributionOutputPath);
			FileInputFormat.addInputPath(wordJoinJob, contextInfoOutputPath);
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
			wordSumJob.setMapOutputValueClass(DoubleWritable.class);
			wordSumJob.setReducerClass(TagContextWordReducer.class);
			wordSumJob.setCombinerClass(TagContextWordReducer.class);
			wordSumJob.setInputFormatClass(SequenceFileInputFormat.class);			
			wordSumJob.setOutputKeyClass(Text.class);
			wordSumJob.setOutputValueClass(DoubleWritable.class);
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
			contextJoinJob.setPartitionerClass(WordJoinPartitioner.class);
			contextJoinJob.setGroupingComparatorClass(WordJoinGroupComparator.class);
			contextJoinJob.setSortComparatorClass(WordJoinSortComparator.class);
			contextJoinJob.setInputFormatClass(SequenceFileInputFormat.class);
			contextJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			contextJoinJob.setOutputKeyClass(Text.class);
			contextJoinJob.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(contextJoinJob, wordSumOutputPath);
			FileInputFormat.addInputPath(contextJoinJob, wordInfoOutputPath);
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
			contextSumJob.setMapOutputValueClass(DoubleWritable.class);
			contextSumJob.setReducerClass(ContextSumReducer.class);
			contextSumJob.setCombinerClass(TagContextWordReducer.class);
			contextSumJob.setInputFormatClass(SequenceFileInputFormat.class);
			contextSumJob.setOutputFormatClass(SeqOutFormatContext.class);
			contextSumJob.setOutputKeyClass(Text.class);
			contextSumJob.setOutputValueClass(TextDoubleWritable.class);			
			FileInputFormat.addInputPath(contextSumJob, contextJoinOutputPath);
			FileOutputFormat.setOutputPath(contextSumJob, contextSumOutputPath);
			
			succeeded = contextSumJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Seventh job failed");
				System.exit(7);
			}
			
			Job normalizingSumJob = new Job(conf, "normalize sum");
			normalizingSumJob.setJarByClass(LearningAlgorithm.class);
			normalizingSumJob.setMapperClass(NormalizingSumMapper.class);
			normalizingSumJob.setMapOutputKeyClass(Text.class);
			normalizingSumJob.setMapOutputValueClass(TextDoubleWritable.class);
			normalizingSumJob.setCombinerClass(NormalizingSumReducer.class);
			normalizingSumJob.setReducerClass(NormalizingSumReducer.class);
			normalizingSumJob.setInputFormatClass(SequenceFileInputFormat.class);
			normalizingSumJob.setOutputFormatClass(SeqOutInitDist.class);
			normalizingSumJob.setOutputKeyClass(Text.class);
			normalizingSumJob.setOutputValueClass(TextDoubleWritable.class);
			FileInputFormat.addInputPath(normalizingSumJob, contextSumOutputPath);
			FileOutputFormat.setOutputPath(normalizingSumJob, normalizeSumPath);
			
			succeeded = normalizingSumJob.waitForCompletion(true);
			if (!succeeded) {
				System.err.println("Eight job failed");
				System.exit(8);
			}
			
			Job normalizingJob = new Job(conf, "normalize");
			normalizingJob.setJarByClass(LearningAlgorithm.class);
			normalizingJob.setMapperClass(NormalizeTaggingMapper.class);
			normalizingJob.setMapOutputKeyClass(Text.class);
			normalizingJob.setMapOutputValueClass(TextTaggedValue.class);
			normalizingJob.setPartitionerClass(WordJoinPartitioner.class);
			normalizingJob.setGroupingComparatorClass(WordJoinGroupComparator.class);
			normalizingJob.setSortComparatorClass(WordJoinSortComparator.class);
			normalizingJob.setReducerClass(NormalizingReducer.class);
			normalizingJob.setInputFormatClass(SequenceFileInputFormat.class);
			normalizingJob.setOutputFormatClass(SeqOutInitDist.class);			
			normalizingJob.setOutputKeyClass(Text.class);
			normalizingJob.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(normalizingJob, normalizeSumPath);
			FileInputFormat.addInputPath(normalizingJob, contextSumOutputPath);
			FileOutputFormat.setOutputPath(normalizingJob, normalizeOutputPath);
			
			succeeded = normalizingJob.waitForCompletion(true);
			
			if (!succeeded) {
				System.err.println("Ninth job failed");
				System.exit(9);
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
			if (!fs.delete(normalizeSumPath, true)) {
				System.out.println("Error deleting normalize sum.");
			}			
			if (!fs.rename(normalizeOutputPath, initialDistributionOutputPath)) {
				System.out.println("Error renaming normalize output.");
			}
		}

		if (!fs.rename(initialDistributionOutputPath, normalizeOutputPath)) {
			System.out.println("Error renaming final normalized output.");
		}
		
		Job finalOutputJob = new Job(conf, "final output");
		finalOutputJob.setJarByClass(LearningAlgorithm.class);
		finalOutputJob.setInputFormatClass(SequenceFileInputFormat.class);
		finalOutputJob.setOutputFormatClass(TextOutputFormat.class);
		finalOutputJob.setMapperClass(FinalOutputMapper.class);
		finalOutputJob.setMapOutputKeyClass(Text.class);
		finalOutputJob.setMapOutputValueClass(TextTaggedValue.class);
		finalOutputJob.setReducerClass(FinalOutputReducer.class);
		finalOutputJob.setOutputKeyClass(Text.class);
		finalOutputJob.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(finalOutputJob, initialDistributionOutputPath);
		FileInputFormat.addInputPath(finalOutputJob, normalizeOutputPath);
		FileOutputFormat.setOutputPath(finalOutputJob, finalOutputPath);
		
		succeeded = finalOutputJob.waitForCompletion(true);
		
		if (!succeeded) {
			System.err.println("Tenth job failed");
			System.exit(9);
		}			
	}
}
