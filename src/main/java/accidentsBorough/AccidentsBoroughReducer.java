package accidentsBorough;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * [(Week, Year, Borough), 1 (letal) or 0 (non letal)] -> [(Week, Year, Borough), count(), sum()]
 * @author Simone Ripamonti
 *
 */
public class AccidentsBoroughReducer extends MapReduceBase
		implements Reducer<WeekBoroughWritable, IntWritable, WeekBoroughWritable, AccidentsAndLethalAccidents> {
	// as daily avg of incidents and lethal incidents per week per borough

	public void reduce(WeekBoroughWritable key, Iterator<IntWritable> values,
			OutputCollector<WeekBoroughWritable, AccidentsAndLethalAccidents> output, Reporter reporter)
			throws IOException {
		int accidents = 0;
		int lethal_accidents = 0;
		while (values.hasNext()) {
			accidents++;
			lethal_accidents += values.next().get();
		}
//		float avg_accidents = ((float) accidents) / 7;
//		float avg_lethal_accidents = ((float) lethal_accidents) / 7;
		output.collect(key, new AccidentsAndLethalAccidents(accidents, lethal_accidents));
	}

}
