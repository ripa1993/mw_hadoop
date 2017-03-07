package contributingFactors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IncidentsAndDeathsWritable implements Writable {

	private IntWritable incidents;
	private DoubleWritable avgDeath;
	
	public IncidentsAndDeathsWritable(){
		this.incidents = new IntWritable();
		this.avgDeath = new DoubleWritable();
	}

	public IncidentsAndDeathsWritable(int incident, double avg){
		this.incidents = new IntWritable(incident);
		this.avgDeath = new DoubleWritable(avg);
	}
	
	public void readFields(DataInput in) throws IOException {
		incidents.readFields(in);
		avgDeath.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		incidents.write(out);
		avgDeath.write(out);
	}
	
	public String toString(){
		String val = String.format("%.10f", avgDeath.get());
		return incidents.get()+"\t"+val;
	}

}
