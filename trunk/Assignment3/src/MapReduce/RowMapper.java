package MapReduce;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class RowMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    private static final Log LOG = LogFactory.getLog("RowMapper");
    private static final Pattern ROW_PATTERN = 
    	Pattern.compile("^([^\\t]+)\\t([\\d]+)\\t([\\d]+)\\t([\\d]+)\\t([\\d]+)$");    

    /**
     * Map. Strips extra fields from each n-gram record and emits record with n-gram and occurrence count.
     * 
     * @param key The record number.
     * @param value Tab-delimited n-gram record from corpus.
     * @param context Hadoop context.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
    	Matcher m = ROW_PATTERN.matcher(value.toString());
    	if (m.matches()) {    		    		
    		Text fiveGram = new Text(m.group(1));
    		LongWritable occurrences = new LongWritable(Long.parseLong(m.group(3)));
    		
    		context.write(fiveGram, occurrences);

    	} else {
    		LOG.error("Error matching row");    		
    	}       
    }
}
