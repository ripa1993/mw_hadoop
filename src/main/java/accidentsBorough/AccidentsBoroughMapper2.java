package accidentsBorough;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AccidentsBoroughMapper2 extends MapReduceBase implements Mapper<LongWritable,Text,Text, IntWritable>{

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] parts = line.split("\t");
		String borough = parts[0];
		int lethalAccidents = Integer.valueOf(parts[4]);
		System.out.println("DEBUG: map "+borough+"|"+lethalAccidents);
		output.collect(new Text(borough), new IntWritable(lethalAccidents));
	}

}
