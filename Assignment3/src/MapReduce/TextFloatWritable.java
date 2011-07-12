package MapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextFloatWritable implements Writable {
	private Text text;
	private FloatWritable value;
	
	public TextFloatWritable()
	{
		this.text = new Text();
		this.value = new FloatWritable();
	}
	
	public TextFloatWritable(Text t, FloatWritable f)
	{
		this.text = t;
		this.value = f;
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		text.write(out);
		value.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		text.readFields(in);
		value.readFields(in);
	}
	
	@Override
	public String toString() 
	{
		return "Text: " + text.toString() + " Value: " + value.toString();
	}
	
	public Text getText()
	{
		return this.text;
	}
	
	public FloatWritable getValue()
	{
		return this.value;
	}
}
