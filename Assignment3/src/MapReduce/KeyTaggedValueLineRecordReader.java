package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
 
 
public class KeyTaggedValueLineRecordReader extends RecordReader<Text, TextTaggedValue> {
 
  // This record reader parses the input file into pairs of text key, and tagged value text,
  // where the tag is based on the name of the input file
  private KeyValueLineRecordReader lineReader;
  private Text lineKey, lineValue;
  private Text key;
  private Text tag;
  private TextTaggedValue value;
 
  public static RecordReader<Text, TextTaggedValue> create(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    RecordReader<Text, TextTaggedValue> ret = new KeyTaggedValueLineRecordReader();
    System.out.println("IM HERE");
    ret.initialize(split,context);
    return ret;
  }
 
  private KeyTaggedValueLineRecordReader() throws IOException {
  }
 
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      org.apache.hadoop.mapreduce.lib.input.FileSplit fs = (org.apache.hadoop.mapreduce.lib.input.FileSplit)split;
      key = new Text(); 
      tag = new Text(fs.getPath().getName().split("-")[0]);
      System.out.println(tag);
      value = new TextTaggedValue(tag);
      lineReader = new KeyValueLineRecordReader(context.getConfiguration(),
            new org.apache.hadoop.mapred.FileSplit(fs.getPath(),fs.getStart(),fs.getLength(),fs.getLocations())); 
      lineKey = lineReader.createKey();
      lineValue = lineReader.createValue();
  }
 
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.next(lineKey, lineValue)) {
      return false;
    }
    String firstpart = lineKey.toString().split("-,-")[0];
    key.set(firstpart);
    System.out.println(lineValue.toString() + " LOLZ");
    //value.setValue(new FloatWritable(Float.parseFloat(lineValue.toString())));
    value.setValue(new FloatWritable(Float.parseFloat(("3.4"))));
    return true;
  }
 
  public TextTaggedValue getCurrentValue() throws IOException, InterruptedException {
    return value;
  }
 
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }
 
  public Text createKey() {
    return new Text("");
  }
 
  public Text createValue() {
    return new Text("");
 }
 
  public long getPos() throws IOException {
      return lineReader.getPos();
  }
 
  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }
 
  public void close() throws IOException {
    lineReader.close();
  }
}