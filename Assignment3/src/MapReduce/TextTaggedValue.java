package MapReduce;

import org.apache.hadoop.io.Text;


public class TextTaggedValue extends TaggedValue<Text,TextFloatWritable> {
	public TextTaggedValue() {
		super();
	}

	public TextTaggedValue(Text tag) {
		super(tag);
	}

	public TextTaggedValue(Text tag,TextFloatWritable value) {
		super(tag,value);
	}

	@Override
	protected void init() {
		tag = new Text();
		value = new TextFloatWritable();
	}

}