package MapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordJoinSortComparator extends WritableComparator {
	private static WritableComparator gc = new WordJoinGroupComparator();
	
	public WordJoinSortComparator()
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
		
		int result = gc.compare(a, b);
		if (result == 0) {
			String[] leftTokens = left.toString().split("-,-");
			String[] rightTokens = right.toString().split("-,-");
			
			boolean leftIsTag = leftTokens[0].equals("tag");
			boolean rightIsTag = rightTokens[0].equals("tag");
			
			if (leftIsTag && !rightIsTag) {
				return -1;
			} else if (!leftIsTag && rightIsTag) {
				return 1;
			} else {
				return 0;
			}
		} else {
			return result;
		}
		
			
	}	
}
