import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SequenceFileViewer {

	/**
	 * @param args Command line arguments specifying sequence file to view.
	 * @throws Exception on any error 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			return;
		}

		Configuration conf = new Configuration();

		Path inPath = new Path(args[0]);
		SequenceFile.Reader sfr = new SequenceFile.Reader(
				inPath.getFileSystem(conf), inPath, conf);
		
		Writable key = (Writable) sfr.getKeyClass().newInstance();
		Writable value = (Writable) sfr.getValueClass().newInstance();

		while (sfr.next(key, value)) {
			System.out.println("Key: " + key.toString() + "\tValue: " + value.toString());                  
		}
	}

}
