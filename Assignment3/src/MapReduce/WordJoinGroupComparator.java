package MapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordJoinGroupComparator extends WritableComparator {
	private static WritableComparator tc = WritableComparator.get(Text.class);
	
	public WordJoinGroupComparator()
	{
		super(Text.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		// Group according to non-tag portion of the key.
		Text left = (Text)a;
		Text right = (Text)b;
		
		Text realLeft = new Text(left.toString().split("-,-")[1]);
		Text realRight = new Text(right.toString().split("-,-")[1]);
				
		int result = tc.compare(realLeft, realRight);
				
		return result;
	}	
}
