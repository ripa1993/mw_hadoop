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
		int accidents = 0;
		int lethal = 0;
		while (values.hasNext()) {
			int deaths = values.next().get();
			if(deaths>0){
				lethal++;
			}
			accidents++;
		}
		System.out.println("DEBUG: accidents "+accidents+" deaths "+lethal+" for "+key.toString());
		double avg = (1.0d*lethal)/accidents;
		output.collect(key, new IncidentsAndDeathsWritable(accidents, avg));
		
	}

}
