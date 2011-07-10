package MapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
 
public class ReduceSideJoin {
 
    // This program gets two types of values and produce a join-by-key value sets
    public static class MapClass  extends Mapper<Text,TextTaggedValue,Text,TextTaggedValue> {
        // The map gets a key and tagged value (of 2 types) and emits the key and the value
        public void map(Text key, TextTaggedValue value, Context context) throws IOException,  InterruptedException {
            context.write(key, value);
        }
    }
 
 
    public static class ReduceClass  extends Reducer<Text,TextTaggedValue,Text,Text> {
        public void reduce(Text key, Iterable<TextTaggedValue> taggedValues, Context context) throws IOException, InterruptedException {
            // The reduce gets a key and a set of values of two types (identified by their tags)
            // and generates a cross product of the two types of values
            Map<Text,List<FloatWritable>> mapTag2Values = new HashMap<Text,List<FloatWritable>>();
            for (TextTaggedValue taggedValue : taggedValues) {
                List<FloatWritable> values = mapTag2Values.get(taggedValue.getTag());
                if (values == null) {
                    values = new LinkedList<FloatWritable>();
                    mapTag2Values.put(taggedValue.getTag(),values);
                }
                values.add(taggedValue.getvalue());
            }
            crossProduct(key,mapTag2Values,context);
        }
 
 
        protected void crossProduct(Text key,Map<Text,List<FloatWritable>> mapTag2Values,Context context) throws IOException, InterruptedException {
            // This specific implementation of the cross product, combine the data of the customers and the orders (
            // of a given costumer id).
            FloatWritable tagword = mapTag2Values.get(new Text("tag")).get(0);
            for (FloatWritable order : mapTag2Values.get("'word"))
                context.write(key, new Text(tagword.toString() + "," + order.toString()));
        }
    }
}