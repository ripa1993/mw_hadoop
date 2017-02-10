package accidentsBorough;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class AvgAccidentsAndLetalWritable implements Writable {

	FloatWritable accidents;
	FloatWritable lethal;
	
	public AvgAccidentsAndLetalWritable() {
		this.accidents = new FloatWritable();
		this.lethal = new FloatWritable();
	}
	
	public AvgAccidentsAndLetalWritable(float accidents, float lethal) {
		this.accidents = new FloatWritable(accidents);
		this.lethal = new FloatWritable(lethal);
	}
	
	public void readFields(DataInput in) throws IOException {
		accidents.readFields(in);
		lethal.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		accidents.write(out);
		lethal.write(out);
	}
	
	public String toString(){
		return accidents.toString()+"\t"+lethal.toString();
	}

}
