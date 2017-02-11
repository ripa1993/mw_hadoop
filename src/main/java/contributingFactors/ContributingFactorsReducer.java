package contributingFactors;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ContributingFactorsReducer extends MapReduceBase
		implements Reducer<Text, IntWritable, Text, IncidentsAndDeathsWritable> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IncidentsAndDeathsWritable> output,
			Reporter reporter) throws IOException {
		int count = 0;
		int deaths = 0;
		while (values.hasNext()) {
			deaths += values.next().get();
			count++;
		}
		System.out.println("DEBUG: accidents "+count+" deaths "+deaths+" for "+key.toString());
		float avg = ((float)deaths/(float)count);
		output.collect(key, new IncidentsAndDeathsWritable(count, avg));
		
	}

}
