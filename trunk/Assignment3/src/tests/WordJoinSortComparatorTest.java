package tests;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import MapReduce.WordJoinSortComparator;

public class WordJoinSortComparatorTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testSort() {
		final WritableComparator wc = new WordJoinSortComparator();
		
		String cb = "context-,-b";
		String ta = "tag-,-a";
		
		assertTrue("string compare", cb.compareTo(ta) < 0);
		
		Text tCb = new Text(cb);
		Text tTa = new Text(ta);
		
		assertTrue("compare object", wc.compare(tCb, tTa) > 0);
		
		Text tCa = new Text("context-,-a");
		Text tTb = new Text("tag-,-b");
		
		Text[] in = { tCb, tTa, tCa, tTb };
		Text[] expected = { tTa, tCa, tTb, tCb };
		
		Arrays.sort(in, wc);
		assertArrayEquals("array sort", expected, in);
		
		
		for (int i = 0; i < 10; i++) {
			Collections.shuffle(Arrays.asList(in));
			Arrays.sort(in, wc);
			assertArrayEquals("array sort after shuffle", expected, in);
		}
		
		Collections.shuffle(Arrays.asList(in));
		Arrays.sort(in, new Comparator<Text>() {

			@Override
			public int compare(Text o1, Text o2) {
				try {
					ByteArrayOutputStream byos1 = new ByteArrayOutputStream();
					DataOutputStream dos1 = new DataOutputStream(byos1);
					o1.write(dos1);
					ByteArrayOutputStream byos2 = new ByteArrayOutputStream();				
					DataOutputStream dos2 = new DataOutputStream(byos2);
					o2.write(dos2);

					byte[] b1 = byos1.toByteArray();
					byte[] b2 = byos2.toByteArray();

					return wc.compare(b1, 0, b1.length, b2, 0, b2.length);
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}
			
		});
		assertArrayEquals("array byte sort", expected, in);
	}

}
