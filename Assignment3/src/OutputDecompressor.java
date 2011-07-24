import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class OutputDecompressor {
	public static void main(String args[]) throws IOException, ClassNotFoundException {
		if (args.length != 2) {
			System.out.println("Usage: OutputDecompressor <inpath> <outpath>");
			System.exit(1);
		}
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		
		Configuration conf = new Configuration();
		
		InputStream is = inPath.getFileSystem(conf).open(inPath);
		
        Class<?> externalCodec = 
        		OutputDecompressor.class.getClassLoader().loadClass("com.hadoop.compression.lzo.LzoCodec");
        CompressionCodec lzoCodec = (CompressionCodec) ReflectionUtils.newInstance(externalCodec, conf);

		InputStream dis = lzoCodec.createInputStream(is);
		OutputStream os = outPath.getFileSystem(conf).create(outPath);
		
		byte[] buffer = new byte[64 * 1024];
		int bytesRead;
		
		while((bytesRead = dis.read(buffer)) != -1) {
			os.write(buffer, 0, bytesRead);
		}
		
		os.flush();
		os.close();
		dis.close();
	}
}
