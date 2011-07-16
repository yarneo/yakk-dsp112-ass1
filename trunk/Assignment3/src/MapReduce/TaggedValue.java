package MapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException; 
 
import org.apache.hadoop.io.Writable;
 
public abstract class TaggedValue<T extends Writable,V extends Writable> implements Writable {
 
    // An implementation of value with tag, as a writable object
 
    protected T tag;
    protected V value;
 
    TaggedValue() {
        init();
    }
 
    TaggedValue(T tag) {
        this.tag = tag;
        this.value = null;
    }
 
    TaggedValue(T tag,V value) {
        this.tag = tag;
        this.value = value;
    }
 
    protected abstract void init();
 
    @Override
    public void readFields(DataInput data) throws IOException {
        tag.readFields(data);
        value.readFields(data);
    }
 
    @Override
    public void write(DataOutput data) throws IOException {
        tag.write(data);
        value.write(data);
    }
    
 
    public String toString() {
        return tag + ":" + value;
    }

    public T getTag() { return tag; }
    public V getValue() { return value; }
    public void setValue(V value) { this.value = value; }
}