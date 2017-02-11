package contributingFactors;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import common.CSV;

public class ContributingFactorsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// skip header of csv file
		if (key.get() == 0) {
			System.out.println("DEBUG: hedaer discarded @" + key.toString());
			return;
		}
		String line = value.toString();
		String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		// discard not legal lines
		if (parts.length < 29) {
			System.out.print("DEBUG: illegal entry @" + key.toString());
			return;
		}
		// find number of death
		int killed = Integer.valueOf(parts[CSV.NUMBER_OF_PERSONS_KILLED]);

//		int[] factors = { CSV.CONTRIBUTING_FACTOR_VEHICLE_1, CSV.CONTRIBUTING_FACTOR_VEHICLE_2,
//				CSV.CONTRIBUTING_FACTOR_VEHICLE_3, CSV.CONTRIBUTING_FACTOR_VEHICLE_4,
//				CSV.CONTRIBUTING_FACTOR_VEHICLE_5 };
		int[] factors = {CSV.CONTRIBUTING_FACTOR_VEHICLE_1};
		for (int i = 0; i < factors.length; i++) {
			String factor = parts[factors[i]];
			System.out.println("DEBUG: found factor " + factor + " @" + key.toString());
			if (factor.length() > 0) {
				System.out.println("DEBUG: emitted with " + killed);
				output.collect(new Text(factor), new IntWritable(killed));
			}
		}

	}

}
