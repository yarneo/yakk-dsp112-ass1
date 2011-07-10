package MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
 
public class KeyTaggedValueTextInputFormat extends FileInputFormat<Text, TextTaggedValue> {
 
  // An implementation of an InputFormat of a text key and text tagged value
  public RecordReader<Text, TextTaggedValue> createRecordReader(InputSplit split,TaskAttemptContext context) throws IOException, InterruptedException {
    return KeyTaggedValueLineRecordReader.create(split,context);
  }
}