package accidentsBorough;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AccidentsBoroughReducer2 extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		int count = 0;
		int sum = 0;
		while (values.hasNext()) {
			count++;
			sum += values.next().get();
		}
		double avg = (double) sum / (double) count;
		System.out.println("DEBUG: reduce "+key.toString()+"|"+avg);
		output.collect(key, new DoubleWritable(avg));
	}

}
