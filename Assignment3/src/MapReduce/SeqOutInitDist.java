package MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A SequenceFileOutputFormat that names output files with a "tag" prefix.
 * 
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class SeqOutInitDist<K,V> extends SequenceFileOutputFormat<K,V> {
	public RecordWriter<K, V> 
	getRecordWriter(TaskAttemptContext context
			) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		if (getCompressOutput(context)) {
			// find the kind of compression to do
			compressionType = getOutputCompressionType(context);

			// find the right codec
			Class<?> codecClass = getOutputCompressorClass(context, 
					DefaultCodec.class);
			codec = (CompressionCodec) 
					ReflectionUtils.newInstance(codecClass, conf);
		}
		// get the path of the temporary output file 
		FileOutputCommitter committer = 
				(FileOutputCommitter) getOutputCommitter(context);
		Path file = new Path(committer.getWorkPath(), getUniqueFile(context, "tag", 
				""));
		FileSystem fs = file.getFileSystem(conf);
		final SequenceFile.Writer out = 
				SequenceFile.createWriter(fs, conf, file,
						context.getOutputKeyClass(),
						context.getOutputValueClass(),
						compressionType,
						codec,
						context);

		return new RecordWriter<K, V>() {

			public void write(K key, V value)
					throws IOException {

				out.append(key, value);
			}

			public void close(TaskAttemptContext context) throws IOException { 
				out.close();
			}
		};
	}


	public void checkOutputSpecs(JobContext job
			) throws IOException{
		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set.");
		}
	}

}
