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
		float avg = deaths/count;
		output.collect(key, new IncidentsAndDeathsWritable(count, avg));
		
	}

}
