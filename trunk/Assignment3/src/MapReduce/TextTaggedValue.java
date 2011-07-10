package MapReduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class TextTaggedValue extends TaggedValue<Text,FloatWritable> {
	public TextTaggedValue() {
		super();
	}

	public TextTaggedValue(Text tag) {
		super(tag);
	}

	public TextTaggedValue(Text tag,FloatWritable value) {
		super(tag,value);
	}

	@Override
	protected void init() {
		tag = new Text();
		value = new FloatWritable();
	}

}