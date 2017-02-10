package contributingFactors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IncidentsAndDeathsWritable implements Writable {

	private IntWritable incidents;
	private FloatWritable avgDeath;
	
	public IncidentsAndDeathsWritable(){
		this.incidents = new IntWritable();
		this.avgDeath = new FloatWritable();
	}

	public IncidentsAndDeathsWritable(int incident, float avgDeath){
		this.incidents = new IntWritable(incident);
		this.avgDeath = new FloatWritable(avgDeath);
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
		return incidents.toString()+"\t"+avgDeath.toString();
	}

}
