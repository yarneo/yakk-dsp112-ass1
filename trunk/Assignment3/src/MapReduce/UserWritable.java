package MapReduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
 
 
public class UserWritable implements Writable {
 
    private LongWritable frequency;
    private ContextInfoArray contexts;
 
    public UserWritable() {
    	this.frequency = new LongWritable();
    	this.contexts = new ContextInfoArray();
    }
    
    public UserWritable(LongWritable frequency) {
        this.frequency = frequency;
        this.contexts = new ContextInfoArray();
    }
 
    public UserWritable(LongWritable frequency,ContextInfoArray contexts) {
        this.frequency = frequency;
        this.contexts = contexts;
    }
    
    public UserWritable(UserWritable other) {
    	this.frequency = new LongWritable(other.frequency.get());
    	Pair[] ConUsWriArr = new Pair[other.contexts.get().length];
    	for(int i=0;i<other.contexts.get().length;i++) {
    		ConUsWriArr[i] = new Pair((Pair)other.contexts.get()[i]);
    	}
    	this.contexts = new ContextInfoArray(ConUsWriArr);
    	
    }
 
    @Override
    public void readFields(DataInput data) throws IOException {
        frequency.readFields(data);
        contexts.readFields(data);
    }
 
    @Override
    public void write(DataOutput data) throws IOException {
    	frequency.write(data);
    	contexts.write(data);
    }
 
    public LongWritable getFrequency() { return frequency; }
    public ContextInfoArray getContexts() { return contexts; }
    public void setFrequency(LongWritable frequency) { this.frequency = frequency; }
    public void setContexts(ContextInfoArray contexts) { this.contexts = contexts; }
    
    public String toString() {
    	String str2 = "";
    	String[] tmp = contexts.toStrings();
    	for(String str : tmp) {
    		str2 += str + ",";
    	}
       return "Freq: " + frequency.toString() + " Contexts: " + str2;   
      }
    
}