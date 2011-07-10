package MapReduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
 
public class Pair implements Writable {
 
    private Text text;
    private LongWritable num;
 
    Pair() {
    	this.text = new Text();
    	this.num = new LongWritable();
    }
    
    public Pair(Pair other) {
    	this.text = new Text(other.text.getBytes());
    	this.num = new LongWritable(other.num.get());
    }
    
    Pair(Text text) {
        this.text = text;
        this.num = new LongWritable();
    }
 
    Pair(Text text,LongWritable num) {
        this.text = text;
        this.num = num;
    }
 
    @Override
    public void readFields(DataInput data) throws IOException {
        text.readFields(data);
        num.readFields(data);
    }
 
    @Override
    public void write(DataOutput data) throws IOException {
    	text.write(data);
    	num.write(data);
    }
 
    public LongWritable getNum() { return num; }
    public Text getText() { return text; }
    public void setText(Text text) { this.text = text; }
    public void setNum(LongWritable num) { this.num = num; }
    
    public String toString() {
        System.out.println("Text: " + text.toString() + " Num: " + num.toString());
        return "Text: " + text.toString() + " Num: " + num.toString();
       }
}