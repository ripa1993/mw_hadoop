package lethalAccidents;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class WeekYearWritable implements WritableComparable<WeekYearWritable> {

	private IntWritable week;
	private IntWritable year;

	public WeekYearWritable() {
		this.week = new IntWritable();
		this.year = new IntWritable();
	}

	public WeekYearWritable(int week, int year) {
		this.week = new IntWritable(week);
		this.year = new IntWritable(year);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.week.readFields(in);
		this.year.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.week.write(out);
		this.year.write(out);
	}

	public int compareTo(WeekYearWritable o) {
		if (o == null) {
			throw new NullPointerException();
		}
		if (o == this) {
			return 0;
		}
		if (this.year.compareTo(o.year) > 0) {
			return 1;
		}
		else if (this.year.compareTo(o.year) == 0) {
			return this.week.compareTo(o.week);
		}
		else {
			return -1;
		}
	}
	
	public String toString(){
		return week.toString() +"\t"+year.toString();
	}

}
