package accidentsBorough;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AccidentsBoroughReducer extends MapReduceBase implements Reducer<WeekBoroughWritable, IntWritable, WeekBoroughWritable, AvgAccidentsAndLetalWritable> {
	// as daily avg of incidents and lethal incidents per week per borough

	public void reduce(WeekBoroughWritable key, Iterator<IntWritable> values,
			OutputCollector<WeekBoroughWritable, AvgAccidentsAndLetalWritable> output, Reporter reporter) throws IOException {
		int accidents = 0;
		int lethal_accidents = 0;
		while(values.hasNext()){
			accidents++;
			lethal_accidents+=values.next().get();
		}
		float avg_accidents = accidents / 7;
		float avg_lethal_accidents = lethal_accidents / 7;
		output.collect(key, new AvgAccidentsAndLetalWritable(avg_accidents, avg_lethal_accidents));
	}

}
