import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import MapReduce1.UserWritable;


public class SequenceFileViewer {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			return;
		}
		
		Configuration conf = new Configuration();
		
		Path inPath = new Path(args[0]);
		SequenceFile.Reader sfr = new SequenceFile.Reader(
				inPath.getFileSystem(conf), inPath, conf);
		
		Text key = new Text();
		UserWritable value = new UserWritable();
		
		while (sfr.next(key, value)) {
			System.out.print("Key: " + key.toString() + "\tValue: " + value.toString());			
		}
	}

}
