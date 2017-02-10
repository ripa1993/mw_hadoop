package lethalAccidents;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LethalAccidentsReducer extends MapReduceBase implements Reducer<WeekYearWritable, IntWritable, WeekYearWritable, IntWritable>{

	public void reduce(WeekYearWritable key, Iterator<IntWritable> values, OutputCollector<WeekYearWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		int count = 0;
		while(values.hasNext()){
			// sum lethal accidents by week
			count += values.next().get();
		}
		output.collect(key, new IntWritable(count));
		
	}

}
