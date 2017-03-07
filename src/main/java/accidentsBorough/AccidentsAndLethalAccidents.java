package accidentsBorough;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class AccidentsAndLethalAccidents implements Writable {
	IntWritable accidents;
	IntWritable lethalAccidents;

	public AccidentsAndLethalAccidents() {
		this.accidents = new IntWritable();
		this.lethalAccidents = new IntWritable();
	}

	public AccidentsAndLethalAccidents(int accidents, int lethalAccidents) {
		this.accidents = new IntWritable(accidents);
		this.lethalAccidents = new IntWritable(lethalAccidents);
	}

	public void readFields(DataInput arg0) throws IOException {
		accidents.readFields(arg0);
		lethalAccidents.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		accidents.write(arg0);
		lethalAccidents.write(arg0);
	}
	
	public String toString(){
		return accidents.toString()+"\t"+lethalAccidents.toString();
	}
}
