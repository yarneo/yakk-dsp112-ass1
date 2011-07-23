package tests;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import MapReduce.WordJoinGroupComparator;

public class WordJoinGroupComparatorTest {

	@Test
	public void testWritableComparatorSortsWithoutTag() {
		WritableComparator wc = new WordJoinGroupComparator();
		
		String cb = "context-,-b";
		String ta = "tag-,-a";
		
		assertTrue("string compare", cb.compareTo(ta) < 0);
		
		Text tCb = new Text(cb);
		Text tTa = new Text(ta);
		
		assertTrue("compare object", wc.compare(tCb, tTa) > 0);
		
		Text tCa = new Text("context-,-a");
		
		assertEquals("group equal", 0, wc.compare(tTa, tCa));
		
	}

}
