package MapReduce1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
 
public class ContextsUserWritable implements Writable {
 
    private Text context;
    private LongWritable contextFrequency;
 
    ContextsUserWritable() {
    	this.context = new Text();
    	this.contextFrequency = new LongWritable();
    }
    
    public ContextsUserWritable(ContextsUserWritable other) {
    	this.context = new Text(other.context.getBytes());
    	this.contextFrequency = new LongWritable(other.contextFrequency.get());
    }
    
    ContextsUserWritable(Text context) {
        this.context = context;
        this.contextFrequency = new LongWritable();
    }
 
    ContextsUserWritable(Text context,LongWritable contextFrequency) {
        this.context = context;
        this.contextFrequency = contextFrequency;
    }
 
    @Override
    public void readFields(DataInput data) throws IOException {
        context.readFields(data);
        contextFrequency.readFields(data);
    }
 
    @Override
    public void write(DataOutput data) throws IOException {
    	context.write(data);
    	contextFrequency.write(data);
    }
 
    public LongWritable getFrequency() { return contextFrequency; }
    public Text getContext() { return context; }
    public void setContext(Text context) { this.context = context; }
    public void setFrequency(LongWritable contextFrequency) { this.contextFrequency = contextFrequency; }
    
    public String toString() {
        System.out.println("Ctxt: " + context.toString() + " CtxtFreq: " + contextFrequency.toString());
        return "Ctxt: " + context.toString() + " CtxtFreq: " + contextFrequency.toString();
       }
}