

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class CorpusReader {	
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("Usage: CorpusWriter <outfile>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();

		
		Path outPath =
			new Path(args[0]);
		SequenceFile.Writer sfw = 
			SequenceFile.createWriter(
					outPath.getFileSystem(conf),
					conf, outPath, Text.class, LongWritable.class);
		
		
		sfw.append(new Text("humpty dumpty sat on the"),new LongWritable(10));
		sfw.append(new Text("humpty dumpty sat on a"),new LongWritable(15));
		sfw.append(new Text("lumpy dumpty sat on the"),new LongWritable(5));
		sfw.append(new Text("humpty slumpy sat on the"),new LongWritable(20));
		
		sfw.close();
		
		return;
	}
}
