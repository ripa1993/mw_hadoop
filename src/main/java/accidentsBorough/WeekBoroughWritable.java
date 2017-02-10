package accidentsBorough;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WeekBoroughWritable implements WritableComparable<WeekBoroughWritable> {

	IntWritable week;
	IntWritable year;
	Text borough;
	
	public WeekBoroughWritable(){
		this.week = new IntWritable();
		this.year = new IntWritable();
		this.borough = new Text();
	}
	
	public WeekBoroughWritable(int week, int year, String borough){
		this.week = new IntWritable(week);
		this.year = new IntWritable(year);
		this.borough = new Text(borough);
	}
	
	public void readFields(DataInput in) throws IOException {
		week.readFields(in);
		year.readFields(in);
		borough.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		week.write(out);
		year.write(out);
		borough.write(out);
		
	}

	public int compareTo(WeekBoroughWritable o) {
		if (o == null) {
			throw new NullPointerException();
		}
		if (o == this) {
			return 0;
		}
		if (this.borough.compareTo(o.borough) > 0) {
			return 1;
		}
		else if (this.borough.compareTo(o.borough) == 0) {
			if (this.year.compareTo(o.year) > 0){
				return 1;
			}
			else if (this.year.compareTo(o.year) ==0){
				return this.week.compareTo(week);
			}
			else{
				return -1;
			}
		}
		else {
			return -1;
		}
	}

	public String toString(){
		return borough.toString()+"\t"+week.toString()+"\t"+year.toString();
	}
}
